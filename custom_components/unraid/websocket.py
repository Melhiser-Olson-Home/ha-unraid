"""WebSocket subscription manager for real-time Unraid data updates."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from unraid_api.exceptions import (
    UnraidAPIError,
    UnraidAuthenticationError,
    UnraidConnectionError,
    UnraidTimeoutError,
)
from unraid_api.models import DockerContainerStats

from .const import (
    WS_CONTAINER_STATS_STALL_TIMEOUT,
    WS_INITIAL_RETRY_DELAY,
    WS_MAX_RETRY_DELAY,
    WS_REFRESH_DEBOUNCE_SECONDS,
    WS_RETRY_BACKOFF_FACTOR,
)

if TYPE_CHECKING:
    from unraid_api import UnraidClient

    from .coordinator import UnraidStorageCoordinator, UnraidSystemCoordinator

_LOGGER = logging.getLogger(__name__)

# The container stats subscription streams raw `docker stats` terminal output;
# the first row of each sample cycle carries ANSI control sequences (clear
# screen + cursor home) glued onto the container ID, so that container's stats
# would otherwise be stored under a corrupted key and never match coordinator
# data (always the same container, since the output order is stable).
_ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


@dataclass
class ContainerStatsSnapshot:
    """Snapshot of real-time container stats from WebSocket subscription."""

    stats: dict[str, DockerContainerStats] = field(default_factory=dict)


class UnraidWebSocketManager:
    """
    Manage WebSocket subscriptions for real-time data updates.

    Runs background tasks for each subscription type, pushing updates
    to the relevant coordinators. Automatically reconnects with
    exponential backoff on disconnects.
    """

    def __init__(
        self,
        api_client: UnraidClient,
        system_coordinator: UnraidSystemCoordinator,
        server_name: str,
        storage_coordinator: UnraidStorageCoordinator | None = None,
    ) -> None:
        """Initialize the WebSocket manager."""
        self._api_client = api_client
        self._system_coordinator = system_coordinator
        self._storage_coordinator = storage_coordinator
        self._server_name = server_name
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        self.container_stats: ContainerStatsSnapshot = ContainerStatsSnapshot()
        # Debounce timestamps for WebSocket-triggered coordinator refreshes
        # (leading-edge: first event refreshes immediately, subsequent events
        # within the cooldown window are suppressed)
        self._last_ups_refresh: float = 0.0
        self._last_notification_refresh: float = 0.0
        # Last array state seen via the array_updates subscription. Used to
        # refresh storage data only on actual state transitions (see
        # _handle_array_updates for why this matters for spun-down disks).
        self._last_array_state: str | None = None
        # Silence allowed on the container stats stream before reconnecting;
        # grows on consecutive stalls (see _handle_container_stats).
        self._stats_stall_timeout: float = WS_CONTAINER_STATS_STALL_TIMEOUT

    async def async_start(self) -> None:
        """Start all WebSocket subscriptions as background tasks."""
        if self._running:
            return
        self._running = True
        _LOGGER.info("Starting WebSocket subscriptions for %s", self._server_name)

        self._tasks = [
            asyncio.create_task(
                self._run_subscription("container_stats", self._handle_container_stats),
                name=f"unraid_ws_container_stats_{self._server_name}",
            ),
            asyncio.create_task(
                self._run_subscription("ups_updates", self._handle_ups_updates),
                name=f"unraid_ws_ups_updates_{self._server_name}",
            ),
            asyncio.create_task(
                self._run_subscription(
                    "notification_added",
                    self._handle_notification_added,
                ),
                name=f"unraid_ws_notification_added_{self._server_name}",
            ),
            asyncio.create_task(
                self._run_subscription("array_updates", self._handle_array_updates),
                name=f"unraid_ws_array_updates_{self._server_name}",
            ),
        ]

    async def async_stop(self) -> None:
        """Stop all WebSocket subscriptions and cancel background tasks."""
        if not self._running:
            return
        self._running = False
        _LOGGER.info("Stopping WebSocket subscriptions for %s", self._server_name)

        for task in self._tasks:
            task.cancel()

        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        self.container_stats = ContainerStatsSnapshot()

    async def _run_subscription(
        self,
        name: str,
        handler: Any,
    ) -> None:
        """Run a subscription with automatic reconnection and backoff."""
        retry_delay = WS_INITIAL_RETRY_DELAY

        while self._running:
            try:
                _LOGGER.debug(
                    "Connecting %s WebSocket subscription for %s",
                    name,
                    self._server_name,
                )
                await handler()
                # If handler returns normally (generator exhausted), reconnect
                retry_delay = WS_INITIAL_RETRY_DELAY

            except UnraidAuthenticationError:
                _LOGGER.error(
                    "WebSocket auth failed for %s (%s)",
                    name,
                    self._server_name,
                )
                return

            except (UnraidConnectionError, UnraidTimeoutError, UnraidAPIError) as err:
                if not self._running:
                    return
                _LOGGER.debug(
                    "WebSocket %s disconnected for %s: %s — retrying in %ss",
                    name,
                    self._server_name,
                    err,
                    retry_delay,
                )

            except asyncio.CancelledError:
                return

            except Exception:
                if not self._running:
                    return
                _LOGGER.exception(
                    "Unexpected error in %s WebSocket for %s",
                    name,
                    self._server_name,
                )

            if not self._running:
                return

            # Wait before reconnecting (with backoff)
            try:
                await asyncio.sleep(retry_delay)
            except asyncio.CancelledError:
                return
            retry_delay = min(retry_delay * WS_RETRY_BACKOFF_FACTOR, WS_MAX_RETRY_DELAY)

    def _should_trigger_refresh(self, last_refresh_time: float) -> bool:
        """Return True if enough time has elapsed since the last refresh."""
        return time.monotonic() - last_refresh_time >= WS_REFRESH_DEBOUNCE_SECONDS

    async def _handle_container_stats(self) -> None:
        """
        Process container stats subscription and update coordinator.

        `docker stats` streams a sample every few seconds while any container
        runs, but a connection can stay open and silently stop delivering (no
        error is raised, so nothing reconnects and sensors freeze on their
        last values). This watches for that without sending any requests:

        - While containers run, silence longer than the stall timeout drops
          the frozen stats and returns, so `_run_subscription` reconnects.
        - With no containers running, silence is expected and never triggers
          a reconnect.
        - The timeout doubles on each consecutive stall (capped at
          WS_MAX_RETRY_DELAY) and resets once stats arrive, so a server that
          keeps going quiet can't cause a reconnect loop.
        """
        stream = aiter(self._api_client.subscribe_container_stats())
        pending: asyncio.Future[DockerContainerStats] | None = None
        try:
            while self._running:
                if pending is None:
                    pending = asyncio.ensure_future(anext(stream))
                done, _ = await asyncio.wait(
                    {pending}, timeout=self._stats_stall_timeout
                )
                if not done:
                    if not self._containers_running():
                        continue  # idle server: keep waiting on the same read
                    self._handle_container_stats_stall()
                    break
                try:
                    stats = pending.result()
                except StopAsyncIteration:
                    break
                finally:
                    pending = None
                self._stats_stall_timeout = WS_CONTAINER_STATS_STALL_TIMEOUT
                if stats.id is None:
                    continue
                container_id = _ANSI_ESCAPE_RE.sub("", stats.id)
                self.container_stats.stats[container_id] = stats
        finally:
            if pending is not None:
                pending.cancel()
                with contextlib.suppress(asyncio.CancelledError, StopAsyncIteration):
                    await pending
            aclose = getattr(stream, "aclose", None)
            if aclose is not None:
                await aclose()

    def _containers_running(self) -> bool:
        """Return True if stats are expected (any container is running)."""
        data = self._system_coordinator.data
        if data is None:
            # No container list yet: held stats mean containers were running.
            return bool(self.container_stats.stats)
        return any(c.is_running for c in data.containers or [])

    def _handle_container_stats_stall(self) -> None:
        """Drop frozen container stats and lengthen the next stall timeout."""
        _LOGGER.warning(
            "No container stats from %s for %ss while containers are running; "
            "reconnecting",
            self._server_name,
            int(self._stats_stall_timeout),
        )
        # Stale values are worse than unknown ones until the stream recovers.
        self.container_stats.stats.clear()
        self._stats_stall_timeout = min(
            self._stats_stall_timeout * 2, WS_MAX_RETRY_DELAY
        )

    async def _handle_ups_updates(self) -> None:
        """Process UPS state subscription and trigger system refresh."""
        async for update in self._api_client.subscribe_ups_updates():
            if not self._running:
                break
            _LOGGER.debug(
                "UPS update received for %s: %s",
                self._server_name,
                update,
            )
            if self._should_trigger_refresh(self._last_ups_refresh):
                self._last_ups_refresh = time.monotonic()
                await self._system_coordinator.async_request_refresh()
            else:
                _LOGGER.debug(
                    "UPS update for %s suppressed (debounce cooldown active)",
                    self._server_name,
                )

    async def _handle_notification_added(self) -> None:
        """Process notification subscription and trigger system refresh."""
        async for notification in self._api_client.subscribe_notification_added():
            if not self._running:
                break
            _LOGGER.debug(
                "Notification received for %s: %s — %s",
                self._server_name,
                notification.importance,
                notification.title,
            )
            if self._should_trigger_refresh(self._last_notification_refresh):
                self._last_notification_refresh = time.monotonic()
                await self._system_coordinator.async_request_refresh()
            else:
                _LOGGER.debug(
                    "Notification refresh for %s suppressed (debounce cooldown active)",
                    self._server_name,
                )

    async def _handle_array_updates(self) -> None:
        """
        Process array state subscription and trigger storage refresh.

        This subscription was removed in v2026.4.1 because it triggered
        storage refreshes every ~30 seconds, waking spun-down disks and
        loading the CPU (#211, #206). It is re-introduced for #247 with
        stricter guards:

        - The server emits periodic heartbeat events with ``state=None``
          (observed every ~30 s on API v4.35 with a stable array). These
          carry no state change and are ignored entirely.
        - A refresh is requested only when the reported state differs from
          the previously seen state (an actual array start/stop), so a
          stable array never causes WebSocket-driven storage polling. At
          that moment disks are active anyway, so no unexpected wake-ups.
        - Rapid transitions (e.g. stopping → stopped) are coalesced by the
          storage coordinator's built-in request_refresh debouncer, which
          never drops the trailing event — the final state always lands.
        """
        async for update in self._api_client.subscribe_array_updates():
            if not self._running:
                break
            if self._storage_coordinator is None:
                continue
            # Heartbeat event — no state change, refreshing would spin up disks.
            if update.state is None:
                continue
            if update.state == self._last_array_state:
                continue
            previous_state = self._last_array_state
            self._last_array_state = update.state
            _LOGGER.debug(
                "Array state changed for %s: %s -> %s — refreshing storage data",
                self._server_name,
                previous_state,
                update.state,
            )
            await self._storage_coordinator.async_request_refresh()
