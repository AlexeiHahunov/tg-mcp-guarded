"""No-network regressions for the account-wide outgoing message boundary."""

import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from tganalytics.infra import send_pacing, tele_client
from tganalytics.infra.send_pacing import (
    SendPacer,
    SendPacingError,
    configured_min_interval,
    outgoing_message_count,
    pace_outgoing_request,
)


def request(name="SendMessageRequest", module="messages", **attrs):
    cls = type(name, (), {"__module__": f"telethon.tl.functions.{module}"})
    result = cls()
    for key, value in attrs.items():
        setattr(result, key, value)
    return result


class Clock:
    def __init__(self, now=1000.0):
        self.now = now
        self.waits = []

    def __call__(self):
        return self.now

    async def sleep(self, duration):
        self.waits.append(duration)
        self.now += duration
        await asyncio.sleep(0)


@pytest.fixture
def enabled(monkeypatch, tmp_path):
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "30")
    monkeypatch.setenv("TG_SEND_PACING_DIR", str(tmp_path))
    monkeypatch.setenv("TG_SEND_PACING_ACCOUNT_ID", "test-account")
    return tmp_path


@pytest.mark.parametrize("value", ["nan", "inf", "-inf", "-1", "bad", ""])
def test_invalid_configuration_fails_closed(monkeypatch, value):
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", value)
    with pytest.raises(SendPacingError):
        configured_min_interval()


def test_upstream_default_is_disabled(monkeypatch):
    monkeypatch.delenv("TG_SEND_MIN_INTERVAL_SEC", raising=False)
    assert configured_min_interval() == 0
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "0.125")
    assert configured_min_interval() == 0.125


@pytest.mark.parametrize("name", [
    "SendMessageRequest", "SendMediaRequest", "SendInlineBotResultRequest",
    "SendScreenshotNotificationRequest", "StartBotRequest",
    "SendWebViewDataRequest", "SendWebViewResultMessageRequest",
])
def test_one_message_rpc_classification(name):
    assert outgoing_message_count(request(name)) == 1


@pytest.mark.parametrize("name,field", [
    ("ForwardMessagesRequest", "id"),
    ("SendScheduledMessagesRequest", "id"),
    ("SendMultiMediaRequest", "multi_media"),
    ("SendQuickReplyMessagesRequest", "id"),
])
def test_vector_cardinality(name, field):
    assert outgoing_message_count(request(name, **{field: [1]})) == 1
    assert outgoing_message_count(request(name, **{field: [1, 2]})) == 2
    for invalid in (None, [], object()):
        with pytest.raises(SendPacingError):
            outgoing_message_count(request(name, **{field: invalid}))


@pytest.mark.parametrize("name,module", [
    ("GetHistoryRequest", "messages"),
    ("EditMessageRequest", "messages"),
    ("DeleteMessagesRequest", "messages"),
    ("SendTypingRequest", "messages"),
    ("SendReactionRequest", "messages"),
    ("SendPaidReactionRequest", "messages"),
    ("SendVoteRequest", "messages"),
    ("SendCodeRequest", "auth"),
    ("SaveFilePartRequest", "upload"),
])
def test_non_message_rpc_not_paced(name, module):
    assert outgoing_message_count(request(name, module)) == 0


def test_envelopes_and_request_lists_are_inspected():
    send = request()
    wrapped = request("InvokeWithoutUpdatesRequest", "", query=send)
    assert outgoing_message_count(wrapped) == 1
    assert outgoing_message_count([request("GetHistoryRequest"), wrapped]) == 1
    assert outgoing_message_count((wrapped, send)) == 2
    with pytest.raises(SendPacingError):
        outgoing_message_count(request("SendUnknownFutureRequest"))


@pytest.mark.asyncio
async def test_shared_state_enforces_interval_after_client_restart(tmp_path):
    clock = Clock()
    starts = []

    async def send():
        starts.append(clock())
        return "sent"

    first = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)
    assert await first.dispatch(send) == "sent"
    restarted = SendPacer(
        tmp_path, "owner", 30, clock=clock, sleep=clock.sleep
    )
    await restarted.dispatch(send)
    await first.dispatch(send)
    assert starts == [1000, 1030, 1060]
    assert clock.waits == [30, 30]
    assert first.state_file.stat().st_mode & 0o777 == 0o600
    assert first.lock_file.stat().st_mode & 0o777 == 0o600


@pytest.mark.asyncio
async def test_account_namespace_and_conservative_default(
    enabled, monkeypatch
):
    interval, directory, account = send_pacing.send_pacing_config()
    assert (interval, directory, account) == (30, enabled, "test-account")
    monkeypatch.delenv("TG_SEND_PACING_ACCOUNT_ID")
    assert send_pacing.send_pacing_config()[2] == "shared-account"
    a = SendPacer(enabled, "a", 30)
    assert a.state_file == SendPacer(enabled, "a", 30).state_file
    assert a.state_file != SendPacer(enabled, "b", 30).state_file


@pytest.mark.asyncio
async def test_failed_send_consumes_slot(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)

    async def fail():
        raise OSError("fake connection failure")

    with pytest.raises(OSError):
        await pacer.dispatch(fail)
    await pacer.dispatch(lambda: asyncio.sleep(0))
    assert clock.waits == [30]
    assert json.loads(pacer.state_file.read_text())["last_attempt_ts"] == 1030


@pytest.mark.asyncio
async def test_timeout_cancels_dispatch_and_releases_lock(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)

    async def never_returns():
        await asyncio.Event().wait()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(pacer.dispatch(never_returns), timeout=0.02)
    # A new instance can acquire the lock; the attempt is not refunded.
    other = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)
    await asyncio.wait_for(other.dispatch(lambda: asyncio.sleep(0)), timeout=1)
    assert clock.waits == [30]


@pytest.mark.asyncio
async def test_waiter_cancellation_does_not_dispatch(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)
    await pacer.dispatch(lambda: asyncio.sleep(0))
    waiting = asyncio.Event()
    dispatched = []

    async def blocked_sleep(_duration):
        waiting.set()
        await asyncio.Event().wait()

    async def must_not_send():
        dispatched.append(True)

    waiter = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=blocked_sleep)
    task = asyncio.create_task(waiter.dispatch(must_not_send))
    await waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not dispatched
    assert json.loads(pacer.state_file.read_text())["last_attempt_ts"] == 1000
    await pacer.dispatch(lambda: asyncio.sleep(0))
    assert clock.waits == [30]


@pytest.mark.asyncio
async def test_corrupt_state_fails_closed_without_network(tmp_path):
    pacer = SendPacer(tmp_path, "owner", 30)
    pacer.state_file.write_text("not-json")
    dispatched = []

    async def send():
        dispatched.append(True)

    with pytest.raises(SendPacingError):
        await pacer.dispatch(send)
    assert not dispatched


@pytest.mark.asyncio
async def test_clock_rollback_waits_conservatively(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)
    await pacer.dispatch(lambda: asyncio.sleep(0))
    clock.now = 990
    await pacer.dispatch(lambda: asyncio.sleep(0))
    assert clock.waits == [40]


@pytest.mark.asyncio
async def test_slow_rpc_completion_starts_a_fresh_interval(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)

    async def slow_send():
        clock.now += 45

    await pacer.dispatch(slow_send)
    assert json.loads(pacer.state_file.read_text())["last_attempt_ts"] == 1045
    await pacer.dispatch(lambda: asyncio.sleep(0))
    assert clock.waits == [30]
    assert clock.now == 1075


@pytest.mark.asyncio
async def test_missing_lock_support_fails_closed_only_when_enabled(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(send_pacing, "fcntl", None)
    pacer = SendPacer(tmp_path, "owner", 30)
    with pytest.raises(SendPacingError, match="POSIX"):
        await pacer.dispatch(lambda: asyncio.sleep(0))
    disabled = SendPacer(tmp_path, "owner", 0)
    assert await disabled.dispatch(lambda: asyncio.sleep(0, result=3)) == 3


@pytest.mark.asyncio
async def test_cancelling_lock_waiter_releases_file_descriptor(tmp_path):
    if send_pacing.fcntl is None:
        pytest.skip("POSIX flock required")
    pacer = SendPacer(tmp_path, "owner", 0.01)
    started = asyncio.Event()
    release = asyncio.Event()

    async def holder():
        started.set()
        await release.wait()

    holding_task = asyncio.create_task(pacer.dispatch(holder))
    await started.wait()
    waiting_task = asyncio.create_task(
        pacer.dispatch(lambda: asyncio.sleep(0))
    )
    await asyncio.sleep(0.01)
    waiting_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiting_task
    release.set()
    await holding_task
    await asyncio.wait_for(pacer.dispatch(lambda: asyncio.sleep(0)), timeout=1)


@pytest.mark.asyncio
async def test_bulk_rejected_and_read_lists_do_not_create_slots(enabled):
    dispatched = []

    async def operation():
        dispatched.append(True)
        return "ok"

    bulk_requests = (
        [request(), request()],
        request("ForwardMessagesRequest", id=[1, 2]),
        request("SendMultiMediaRequest", multi_media=[1, 2]),
        request("SendScheduledMessagesRequest", id=[1, 2]),
    )
    for bulk in bulk_requests:
        with pytest.raises(SendPacingError, match="Bulk"):
            await pace_outgoing_request(bulk, operation)
    assert not dispatched
    assert await pace_outgoing_request(
        [request("GetHistoryRequest"), request("EditMessageRequest")],
        operation,
    ) == "ok"
    assert list(enabled.iterdir()) == []


@pytest.mark.asyncio
async def test_disabled_pacing_preserves_upstream_bulk_behavior(
    enabled, monkeypatch
):
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "0")
    assert await pace_outgoing_request(
        [request(), request()], lambda: asyncio.sleep(0, result=7)
    ) == 7
    assert list(enabled.iterdir()) == []


@pytest.mark.asyncio
async def test_invalid_send_config_does_not_break_reads(enabled, monkeypatch):
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "invalid")
    assert await pace_outgoing_request(
        request("GetHistoryRequest"), lambda: asyncio.sleep(0, result="read")
    ) == "read"
    with pytest.raises(SendPacingError):
        await pace_outgoing_request(request(), lambda: asyncio.sleep(0))
    assert list(enabled.iterdir()) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("name,extra", [
    ("SendMessageRequest", {}),
    ("SendMediaRequest", {}),
    ("SendInlineBotResultRequest", {}),
    ("ForwardMessagesRequest", {"id": [1]}),
])
async def test_scheduled_sends_cannot_form_a_deferred_burst(
    enabled, monkeypatch, name, extra
):
    rpc = request(name, schedule_date=9999999999, **extra)
    with pytest.raises(SendPacingError, match="Scheduled"):
        await pace_outgoing_request(rpc, lambda: asyncio.sleep(0))
    assert list(enabled.iterdir()) == []
    # Compatibility: upstream opt-out preserves Telegram's scheduling feature.
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "0")
    assert await pace_outgoing_request(
        rpc, lambda: asyncio.sleep(0, result="scheduled")
    ) == "scheduled"


@pytest.mark.asyncio
async def test_guarded_client_common_dispatch_covers_direct_call(
    enabled, monkeypatch
):
    calls = []

    async def parent_call(self, sender, rpc, *args, **kwargs):
        calls.append(rpc)
        return "fake-network-result"

    monkeypatch.setattr(tele_client.TelegramClient, "_call", parent_call)
    monkeypatch.setattr(tele_client, "WRITE_CONTEXT", "actions_mcp")
    monkeypatch.setattr(tele_client, "_is_actions_process", lambda: True)
    monkeypatch.setattr(tele_client, "_is_direct_write_allowed", lambda: True)
    client = object.__new__(tele_client.GuardedTelegramClient)
    rpc = request()
    assert await client._call(None, rpc) == "fake-network-result"
    assert calls == [rpc]
    assert len(list(enabled.glob("*.json"))) == 1
    with pytest.raises(SendPacingError):
        await client._call(None, [request(), request()])
    assert calls == [rpc]


@pytest.mark.asyncio
async def test_read_profile_and_non_message_calls_do_not_pace(
    enabled, monkeypatch
):
    async def parent_call(self, sender, rpc, *args, **kwargs):
        return "ok"

    monkeypatch.setattr(tele_client.TelegramClient, "_call", parent_call)
    monkeypatch.setattr(tele_client, "WRITE_CONTEXT", "read_mcp")
    monkeypatch.setattr(tele_client, "_is_actions_process", lambda: False)
    client = object.__new__(tele_client.GuardedTelegramClient)
    assert await client._call(None, request("GetHistoryRequest")) == "ok"
    monkeypatch.setattr(tele_client, "WRITE_CONTEXT", "actions_mcp")
    monkeypatch.setattr(tele_client, "_is_actions_process", lambda: True)
    assert await client._call(None, request("GetHistoryRequest")) == "ok"
    assert list(enabled.iterdir()) == []


def test_real_processes_share_persistent_send_slots(tmp_path):
    """Three independent runtimes cannot each take a fresh first-send slot."""
    if send_pacing.fcntl is None:
        pytest.skip("POSIX flock required")
    package = Path(__file__).resolve().parents[1] / "tganalytics"
    code = (
        "import asyncio,time,sys; from pathlib import Path; "
        "from tganalytics.infra.send_pacing import SendPacer; "
        "p=SendPacer(Path(sys.argv[1]),'same-owner',0.12); "
        "exec('async def op():\\n print(time.time(),flush=True)'); "
        "asyncio.run(p.dispatch(op))"
    )
    env = dict(os.environ, PYTHONPATH=str(package))
    children = [
        subprocess.Popen(
            [sys.executable, "-c", code, str(tmp_path)],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env,
        ) for _ in range(3)
    ]
    timestamps = []
    try:
        for child in children:
            out, error = child.communicate(timeout=10)
            assert child.returncode == 0, error
            timestamps.append(float(out.strip()))
    finally:
        for child in children:
            if child.poll() is None:
                child.kill()
                child.wait()
    timestamps.sort()
    assert min(b - a for a, b in zip(timestamps, timestamps[1:])) >= 0.11


def test_crashed_process_leaves_its_reserved_slot(tmp_path):
    if send_pacing.fcntl is None:
        pytest.skip("POSIX flock required")
    package = Path(__file__).resolve().parents[1] / "tganalytics"
    code = (
        "import asyncio,os,sys,time; from pathlib import Path; "
        "from tganalytics.infra.send_pacing import SendPacer; "
        "p=SendPacer(Path(sys.argv[1]),'owner',30,"
        "clock=lambda:time.time()-60); "
        "exec('async def op():\\n os._exit(17)'); "
        "asyncio.run(p.dispatch(op))"
    )
    child = subprocess.run([sys.executable, "-c", code, str(tmp_path)],
                           capture_output=True, text=True, timeout=10,
                           env=dict(os.environ, PYTHONPATH=str(package)))
    assert child.returncode == 17, child.stderr
    pacer = SendPacer(tmp_path, "owner", 30)
    old_state = json.loads(pacer.state_file.read_text())
    assert old_state["in_flight"] is True
    # Simulate a killed long RPC: its initial attempt is already older than
    # the interval. The restart still must impose a fresh 30-second cooldown.
    clock = Clock(old_state["last_attempt_ts"] + 60)
    restarted = SendPacer(
        tmp_path, "owner", 30, clock=clock, sleep=clock.sleep
    )
    asyncio.run(restarted.dispatch(lambda: asyncio.sleep(0)))
    assert clock.waits == [30]
    assert json.loads(pacer.state_file.read_text())["in_flight"] is False


@pytest.mark.asyncio
async def test_cancelled_crash_recovery_cannot_refund_cooldown(tmp_path):
    clock = Clock()
    pacer = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=clock.sleep)
    pacer._save_attempt(100, in_flight=True)
    waiting = asyncio.Event()

    async def block(_duration):
        waiting.set()
        await asyncio.Event().wait()

    recovering = SendPacer(tmp_path, "owner", 30, clock=clock, sleep=block)
    task = asyncio.create_task(recovering.dispatch(lambda: asyncio.sleep(0)))
    await waiting.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert json.loads(pacer.state_file.read_text())["in_flight"] is True
    clock.now += 60
    await pacer.dispatch(lambda: asyncio.sleep(0))
    assert clock.waits == [30]
