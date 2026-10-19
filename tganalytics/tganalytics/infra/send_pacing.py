"""Durable account-wide pacing of outgoing message RPCs.

This is separate from API RPS and preview approval age.  The lock is held
through dispatch, and an attempted send is persisted before dispatch.  A
failed, cancelled or crashed sender therefore cannot refund its time slot.
Only Action MCP opts into this guard; upstream defaults to disabled.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time
from typing import Awaitable, Callable, TypeVar

from .paths import resolve_state_path

try:
    import fcntl
except ImportError:  # pragma: no cover - enabled pacing fails closed
    fcntl = None


T = TypeVar("T")


class SendPacingError(RuntimeError):
    """The pacing guard cannot safely dispatch this request."""


# These Send* RPCs update transient/chat state without publishing a message.
_NON_MESSAGE_SENDS = {
    "SendTypingRequest",
    "SendReactionRequest",
    "SendPaidReactionRequest",
    "SendVoteRequest",
}

_SINGLE_MESSAGE_SENDS = {
    "SendMessageRequest",
    "SendMediaRequest",
    "SendInlineBotResultRequest",
    "SendScreenshotNotificationRequest",
    "SendWebViewDataRequest",
    "SendWebViewResultMessageRequest",
    "StartBotRequest",
}
_VECTOR_MESSAGE_SENDS = {
    "ForwardMessagesRequest": "id",
    "SendScheduledMessagesRequest": "id",
    "SendMultiMediaRequest": "multi_media",
    "SendQuickReplyMessagesRequest": "id",
}


def outgoing_message_count(request: object) -> int:
    """Count message-producing RPCs, unwrapping Invoke* transport envelopes.

    Unknown messages.Send* RPCs fail closed while pacing is enabled.  A
    vector send with uninspectable cardinality also fails closed.  No message
    text, peer identifier or other payload data is included in errors.
    """
    if isinstance(request, (list, tuple, set)):
        return sum(outgoing_message_count(item) for item in request)
    cls = request.__class__
    module = getattr(cls, "__module__", "")
    if not module.startswith("telethon.tl.functions"):
        return 0
    name = cls.__name__
    if name.startswith("Invoke") or name == "InitConnectionRequest":
        nested = getattr(request, "query", None)
        if nested is None:
            raise SendPacingError("Cannot inspect Telegram request envelope")
        return outgoing_message_count(nested)
    if not module.endswith(".messages"):
        return 0
    if (
        name in _VECTOR_MESSAGE_SENDS or name in _SINGLE_MESSAGE_SENDS
    ) and getattr(request, "schedule_date", None):
        raise SendPacingError(
            "Scheduled sends are blocked by account send pacing"
        )
    if name in _VECTOR_MESSAGE_SENDS:
        items = getattr(request, _VECTOR_MESSAGE_SENDS[name], None)
        if not isinstance(items, (list, tuple)) or not items:
            raise SendPacingError("Cannot inspect outgoing message count")
        return len(items)
    if name in _SINGLE_MESSAGE_SENDS:
        return 1
    if name in _NON_MESSAGE_SENDS:
        return 0
    if name.startswith(("Send", "Forward")):
        raise SendPacingError("Unclassified outgoing Telegram request")
    return 0


def configured_min_interval() -> float:
    """Shared parser for policy metadata and the enforced interval."""
    try:
        interval = float(os.environ.get("TG_SEND_MIN_INTERVAL_SEC", "0"))
    except (TypeError, ValueError) as exc:
        raise SendPacingError("Invalid TG_SEND_MIN_INTERVAL_SEC") from exc
    if not math.isfinite(interval) or interval < 0:
        raise SendPacingError(
            "TG_SEND_MIN_INTERVAL_SEC must be finite and >= 0"
        )
    return interval


def send_pacing_config() -> tuple[float, Path, str]:
    """Read public config without importing a client or credentials."""
    interval = configured_min_interval()
    directory = resolve_state_path(
        os.getenv("TG_SEND_PACING_DIR", ""), "anti_spam"
    )
    account = (
        os.getenv("TG_SEND_PACING_ACCOUNT_ID", "").strip() or "shared-account"
    )
    return interval, directory, account


class SendPacer:
    """One outgoing message per interval across clients/processes/restarts.

    Uses POSIX flock (macOS/Linux). Contention sleeps asynchronously with
    bounded backoff rather than blocking the event loop or spinning. The
    process lock is held through the network attempt, preventing late wakeups
    and long requests from collapsing previously reserved slots into a burst.
    """

    def __init__(
        self,
        directory: Path,
        account: str,
        interval_sec: float,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        interval = float(interval_sec)
        if not math.isfinite(interval) or interval < 0:
            raise SendPacingError("Send interval must be finite and >= 0")
        self.interval_sec = interval
        namespace = hashlib.sha256(
            str(account).encode("utf-8")
        ).hexdigest()[:16]
        self.state_file = Path(directory) / f"send_pacing_{namespace}.json"
        self.lock_file = self.state_file.with_suffix(".lock")
        self.clock = clock
        self.sleep = sleep

    @asynccontextmanager
    async def _locked(self):
        if fcntl is None:
            raise SendPacingError("Enabled send pacing requires POSIX flock")
        self.state_file.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        fd = os.open(self.lock_file, os.O_CREAT | os.O_RDWR, 0o600)
        acquired = False
        try:
            os.fchmod(fd, 0o600)
            backoff = 0.025
            while True:
                try:
                    fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    acquired = True
                    break
                except BlockingIOError:
                    await self.sleep(backoff)
                    backoff = min(1.0, backoff * 2)
            yield
        finally:
            if acquired:
                fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _load_last_attempt(self) -> tuple[float | None, bool]:
        if not self.state_file.exists():
            return None, False
        try:
            raw = json.loads(self.state_file.read_text(encoding="utf-8"))
            value = raw["last_attempt_ts"]
            if isinstance(value, bool):
                raise ValueError("boolean timestamp")
            value = float(value)
            if not math.isfinite(value) or value < 0:
                raise ValueError("invalid timestamp")
            in_flight = raw.get("in_flight", False)
            if not isinstance(in_flight, bool):
                raise ValueError("invalid in-flight marker")
            return value, in_flight
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise SendPacingError(
                "Send pacing state is unreadable or invalid"
            ) from exc

    def _save_attempt(
        self, timestamp: float, *, in_flight: bool = False
    ) -> None:
        fd, temp_path = tempfile.mkstemp(
            prefix="send_pacing_", dir=self.state_file.parent
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({
                    "version": 2,
                    "last_attempt_ts": timestamp,
                    "in_flight": in_flight,
                }, handle)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.state_file)
            # Persist the rename as well as the file before attempting the RPC.
            directory_fd = os.open(self.state_file.parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)

    async def dispatch(self, operation: Callable[[], Awaitable[T]]) -> T:
        if self.interval_sec <= 0:
            return await operation()
        async with self._locked():
            previous, in_flight = self._load_last_attempt()
            if in_flight:
                # We own the OS lock, so the former sender has disappeared.
                # Its last RPC may have completed after its initial timestamp.
                # A fresh full cooldown is safer than treating that old slot
                # as elapsed. Keep the marker until dispatch finishes so a
                # second recovery crash/cancellation cannot erase the cooldown.
                previous = max(previous or 0, self.clock())
                self._save_attempt(previous, in_flight=True)
            if previous is not None:
                while True:
                    remaining = previous + self.interval_sec - self.clock()
                    if remaining <= 0:
                        break
                    await self.sleep(remaining)
            started_at = self.clock()
            self._save_attempt(started_at, in_flight=True)
            try:
                return await operation()
            finally:
                # A slow RPC must not collapse the visible completion-to-next
                # dispatch gap. Cancellation/failure is equally conservative.
                self._save_attempt(max(started_at, self.clock()))


async def pace_outgoing_request(
    request: object,
    operation: Callable[[], Awaitable[T]],
) -> T:
    """Action-MCP caller gate: reject bulk sends, pace one real send RPC."""
    try:
        count = outgoing_message_count(request)
    except SendPacingError:
        if configured_min_interval() <= 0:
            return await operation()
        raise
    if count == 0:
        return await operation()
    interval, directory, account = send_pacing_config()
    if interval <= 0:
        return await operation()
    if count > 1:
        raise SendPacingError(
            "Bulk outgoing messages are blocked by account send pacing; "
            "send one message per request"
        )
    return await SendPacer(directory, account, interval).dispatch(operation)
