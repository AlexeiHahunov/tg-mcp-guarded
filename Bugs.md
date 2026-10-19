# Fixed issues

## Explicit owner-directed recipients could not be configured

Previously disabling the fixed recipient allowlist also triggered the general unsafe
policy guard. `TG_ACTIONS_TARGET_POLICY=owner_directed` now explicitly permits an empty
recipient list with `TG_ACTIONS_REQUIRE_ALLOWLIST=0`, while preserving every unrelated
action guard and each approved lane's target scope. The default remains `allowlist`;
empty, unknown and inconsistent modes fail closed. Regression coverage is in
`tests/test_mcp_actions_target_policy.py`.

## Mature approvals could execute messages back to back

Approval minimum age delays each approved payload relative to its preview; it does
not space messages whose approvals have already matured. Optional
`TG_SEND_MIN_INTERVAL_SEC` now enforces a persisted shared interval at the actual
Actions message/file transport boundary. The generic default remains disabled (`0`)
for compatibility. Regression coverage is in `tests/test_send_pacing.py`.
