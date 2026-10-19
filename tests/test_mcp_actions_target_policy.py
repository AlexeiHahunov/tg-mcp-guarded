"""Explicit owner-directed targets preserve all unrelated action gates."""

import mcp_server_actions as actions
import pytest
from types import SimpleNamespace

from mcp_actions_policy import detect_unsafe_defaults
from tests.test_mcp_actions_write_lane import _configure, _create_lane


def _owner_env():
    return {
        "TG_BLOCK_DIRECT_TELETHON_WRITE": "1",
        "TG_ALLOW_DIRECT_TELETHON_WRITE": "0",
        "TG_ENFORCE_ACTION_PROCESS": "1",
        "TG_ACTIONS_TARGET_POLICY": "owner_directed",
        "TG_ACTIONS_ALLOWED_GROUPS": "",
        "TG_ACTIONS_CONFIRMATION_PHRASE": "test-confirmation",
    }


def _issues(env=None, **overrides):
    options = dict(
        env=_owner_env() if env is None else env,
        require_allowlist=False,
        require_confirmation_text=True,
        require_approval_code=True,
        idempotency_enabled=True,
    )
    options.update(overrides)
    return detect_unsafe_defaults(**options)


def test_owner_directed_explicit_mode_is_accepted_without_unsafe_override():
    assert _issues() == []


@pytest.mark.parametrize("mode", ["", "*", "unknown", "OWNER_DIRECTED"])
def test_unknown_or_empty_mode_is_blocked(mode):
    env = _owner_env()
    env["TG_ACTIONS_TARGET_POLICY"] = mode
    assert any("TG_ACTIONS_TARGET_POLICY" in issue for issue in _issues(env))


def test_missing_mode_preserves_legacy_allowlist_requirement():
    env = _owner_env()
    del env["TG_ACTIONS_TARGET_POLICY"]
    assert any("TG_ACTIONS_REQUIRE_ALLOWLIST" in issue for issue in _issues(env))
    env["TG_ACTIONS_ALLOWED_GROUPS"] = "@existing_target"
    assert _issues(env, require_allowlist=True) == []


def test_owner_directed_requires_both_disabled_flag_and_empty_list():
    assert _issues(require_allowlist=True)
    env = _owner_env()
    env["TG_ACTIONS_ALLOWED_GROUPS"] = "@old_target"
    assert any("must be empty" in issue for issue in _issues(env))


@pytest.mark.parametrize("flag", [
    "require_confirmation_text", "require_approval_code", "idempotency_enabled",
])
def test_owner_directed_does_not_relax_confirmation_approval_or_idempotency(flag):
    assert _issues(**{flag: False})


@pytest.mark.parametrize("key,value", [
    ("TG_BLOCK_DIRECT_TELETHON_WRITE", "0"),
    ("TG_ALLOW_DIRECT_TELETHON_WRITE", "1"),
    ("TG_ENFORCE_ACTION_PROCESS", "0"),
    ("TG_ACTIONS_CONFIRMATION_PHRASE", ""),
    ("TG_ACTIONS_CONFIRMATION_PHRASE", "change-me-local-confirmation-phrase"),
])
def test_owner_directed_does_not_relax_process_or_phrase_guards(key, value):
    env = _owner_env()
    env[key] = value
    assert _issues(env)


def _open_targets(monkeypatch):
    monkeypatch.setattr(actions, "TARGET_POLICY", "owner_directed")
    monkeypatch.setattr(actions, "REQUIRE_ALLOWLIST", False)
    monkeypatch.setattr(actions, "ALLOWED_TARGETS", set())


def test_open_targets_accepts_new_target_but_rejects_empty(monkeypatch):
    _open_targets(monkeypatch)
    assert actions._check_target_allowed("@new_target") == (True, None)
    assert actions._check_target_allowed("-1009999999999") == (True, None)
    for empty in ("", " ", "@"):
        assert actions._check_target_allowed(empty)[0] is False


def test_target_check_fails_closed_on_invalid_mode_configuration(monkeypatch):
    _open_targets(monkeypatch)
    monkeypatch.setattr(actions, "ALLOWED_TARGETS", {"old_target"})
    assert actions._check_target_allowed("@new_target")[0] is False
    monkeypatch.setattr(actions, "ALLOWED_TARGETS", set())
    monkeypatch.setattr(actions, "TARGET_POLICY", "")
    assert actions._check_target_allowed("@new_target")[0] is False


def test_legacy_target_check_still_restricts_targets(monkeypatch):
    monkeypatch.setattr(actions, "TARGET_POLICY", "allowlist")
    monkeypatch.setattr(actions, "REQUIRE_ALLOWLIST", True)
    monkeypatch.setattr(actions, "ALLOWED_TARGETS", {"existing_target"})
    assert actions._check_target_allowed("@existing_target") == (True, None)
    assert actions._check_target_allowed("@new_target")[0] is False


@pytest.mark.asyncio
async def test_policy_reports_explicit_target_mode_and_shared_pacing(monkeypatch, tmp_path):
    _open_targets(monkeypatch)
    monkeypatch.setattr(actions, "get_rate_limiter", lambda: SimpleNamespace(get_stats=lambda: {}))
    monkeypatch.setattr(actions, "ctx", SimpleNamespace(session_path_status=lambda: {"conflict": False}))
    monkeypatch.setattr(actions, "LANE_FILE", tmp_path / "lanes.json")
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "2.5")
    policy = await actions.tg_get_actions_policy()
    assert policy["target_policy"] == "owner_directed"
    assert policy["require_allowlist"] is False
    assert policy["allowed_targets"] == []
    assert policy["send_min_interval_sec"] == 2.5
    assert policy["send_pacing_scope"] == "account"
    monkeypatch.setenv("TG_SEND_MIN_INTERVAL_SEC", "0")
    policy = await actions.tg_get_actions_policy()
    assert policy["send_pacing_scope"] == "disabled"


@pytest.mark.asyncio
async def test_open_targets_keeps_confirmation_approval_delay_and_dry_run(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    _open_targets(monkeypatch)
    monkeypatch.setattr(actions, "APPROVAL_MIN_AGE_SEC", 30)
    args = dict(group="@outside_old_list", message_text="target policy test")
    missing_confirm = await actions.tg_send_message(**args, dry_run=False)
    assert missing_confirm["success"] is False
    assert "confirm=true" in missing_confirm["error"]
    missing_phrase = await actions.tg_send_message(**args, dry_run=False, confirm=True)
    assert missing_phrase["success"] is False
    assert "confirmation_text" in missing_phrase["error"]
    missing_approval = await actions.tg_send_message(
        **args, dry_run=False, confirm=True, confirmation_text="confirm-test-action",
    )
    assert missing_approval["success"] is False
    assert "approval_code" in missing_approval["error"]
    preview = await actions.tg_send_message(**args, dry_run=True, confirm=False)
    assert preview["success"] is True and preview["dry_run"] is True
    assert actions.ctx.manager.sent == []
    too_fresh = await actions.tg_send_message(
        **args, dry_run=False, confirm=True,
        confirmation_text="confirm-test-action", approval_code=preview["approval_code"],
    )
    assert too_fresh["success"] is False
    assert "too fresh" in too_fresh["error"]
    assert actions.ctx.manager.sent == []


@pytest.mark.asyncio
async def test_open_targets_does_not_expand_approved_lane_scope(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    _open_targets(monkeypatch)
    preview = await _create_lane(targets=["@outside_old_list"])
    assert preview["success"] is True
    approved = await actions.tg_approve_write_lane(
        lane_id=preview["lane_id"],
        confirmation_text="confirm-test-action", approval_code=preview["approval_code"],
    )
    assert approved["success"] is True
    result = await actions.tg_send_message_with_lane(
        lane_id=preview["lane_id"], group="@another_target",
        message_text="scope test", dry_run=False,
    )
    assert result["success"] is False
    assert "outside the approved write lane" in result["error"]
    assert actions.ctx.manager.sent == []
