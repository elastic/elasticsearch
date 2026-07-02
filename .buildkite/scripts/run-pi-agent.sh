#!/bin/bash
# Runs a pi-agent workflow session.
# Called by .buildkite/pipelines/agentic-workflow.yml.
#
# All variables (WORKFLOW, ISSUE_URL, PR_URL, BUILDKITE_RETRY_COUNT,
# PI_AGENT_SESSION_DIR) are set as shell environment variables before this
# script runs — no Buildkite YAML interpolation headaches here.

set -euo pipefail

# ── Validate inputs ────────────────────────────────────────────────────────────
case "${WORKFLOW:-}" in
  test-analysis)
    [[ -n "${ISSUE_URL:-}" ]] || { echo "ISSUE_URL is required for test-analysis" >&2; exit 1; }
    ;;
  pull-request-fix)
    [[ -n "${PR_URL:-}" ]] || { echo "PR_URL is required for pull-request-fix" >&2; exit 1; }
    ;;
  pull-request-creation)
    [[ -n "${ISSUE_URL:-}" ]] || { echo "ISSUE_URL is required for pull-request-creation" >&2; exit 1; }
    ;;
  *)
    echo "WORKFLOW is not set or not recognised: '${WORKFLOW:-}'" >&2
    echo "Trigger this pipeline with WORKFLOW set to one of:" >&2
    echo "  test-analysis | pull-request-fix | pull-request-creation" >&2
    echo "From the Buildkite UI: set WORKFLOW in the 'Environment Variables' field of the New Build dialog." >&2
    echo "From the API: include WORKFLOW in the env block of the build request." >&2
    exit 1
    ;;
esac

# ── Session persistence (spot-instance preemption recovery) ───────────────────
SESSION_DIR="${PI_AGENT_SESSION_DIR:-/tmp/pi-agent-sessions}"
mkdir -p "$SESSION_DIR"

_upload_session() {
  local latest
  latest=$(ls -t "$SESSION_DIR"/*.jsonl 2>/dev/null | head -1) || true
  if [[ -n "$latest" ]]; then
    local dest="$SESSION_DIR/pi-agent-session.jsonl"
    # The newest .jsonl may already be the canonical artifact name (e.g. on a
    # resumed retry). Only copy when they differ to avoid a cp self-copy error.
    [[ "$latest" -ef "$dest" ]] || cp "$latest" "$dest"
    buildkite-agent artifact upload "$dest" 2>/dev/null || true
    echo "Session artifact uploaded ($(wc -c < "$dest") bytes)"
  fi
}
trap '_upload_session' EXIT
trap 'echo "SIGTERM received — uploading session before exit"; _upload_session; exit 47' SIGTERM

if [[ "${BUILDKITE_RETRY_COUNT:-0}" -gt 0 ]]; then
  echo "--- Retry #${BUILDKITE_RETRY_COUNT} — downloading previous session"
  buildkite-agent artifact download "pi-agent-session.jsonl" "$SESSION_DIR/" 2>/dev/null \
    && echo "Session downloaded ($(wc -c < "$SESSION_DIR/pi-agent-session.jsonl") bytes) — will resume" \
    || echo "No previous session artifact found — starting fresh"
fi

# ── Opening annotation ─────────────────────────────────────────────────────────
REF="${ISSUE_URL:-${PR_URL:-}}"
buildkite-agent annotate \
  "### 🤖 pi-agent starting

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}
Session is initialising…" \
  --context "pi-agent-progress" --style "info"

# ── Run pi-agent (nono sandbox) ────────────────────────────────────────────────
# pi-agent drives git, gh, Gradle, and LLM APIs — run it with least-privilege
# filesystem access. Network is left open (LiteLLM, GitHub, Buildkite, GE all
# need outbound HTTPS). --startup-timeout 0 skips the TUI-detection heuristic
# since pi-agent is non-interactive. --silent suppresses the nono banner so CI
# logs stay readable.
# Filesystem grants, network allowlist, and vault command block live in the
# bundled nono-pi-agent.json profile (ships with the pi-agent distro).
# Only SESSION_DIR is dynamic at runtime and must stay here as a CLI flag.
NONO_ARGS=(
  --profile "${HOME}/.local/pi-agent/nono-pi-agent.json"
  --allow "${SESSION_DIR}"   # runtime path for JSONL session snapshots; not known at profile-authoring time
  --startup-timeout 0        # non-interactive — skip TUI-readiness check
  --silent                   # suppress nono banner/summary in CI logs
)

PI_EXIT=0
case "${WORKFLOW}" in
  test-analysis)
    nono run "${NONO_ARGS[@]}" -- pi-agent analyze --issue-url "${ISSUE_URL}" || PI_EXIT=$? ;;
  pull-request-fix)
    nono run "${NONO_ARGS[@]}" -- pi-agent fix-pr  --pr-url    "${PR_URL}"    || PI_EXIT=$? ;;
  pull-request-creation)
    nono run "${NONO_ARGS[@]}" -- pi-agent create  --issue-url "${ISSUE_URL}" || PI_EXIT=$? ;;
esac

# ── Nono sandbox summary ─────────────────────────────────────────────────────
# Post a Buildkite annotation listing everything nono blocked during this run.
# The audit log is written by default; we fetch the most-recent session (CI
# serialises to concurrency=1 so there is no ambiguity) and extract denials.
_nono_audit_annotation() {
  command -v nono &>/dev/null || return
  command -v jq   &>/dev/null || return

  local session_id
  session_id=$(
    nono audit list --recent 1 --json 2>/dev/null \
      | jq -r 'if type == "array" then .[0].id else (.sessions // [])[0].id end // empty' \
      2>/dev/null
  )
  [[ -z "${session_id:-}" ]] && return

  local audit_json
  audit_json=$(nono audit show "${session_id}" --json 2>/dev/null) || return

  local denials net_blocks
  denials=$(
    echo "${audit_json}" | jq -r '
      [ .audit_events[]?
        | select(.decision == "deny"
              or ((.type // "") | test("den(y|ied)"; "i")))
      ]
      | if length == 0 then empty
        else "**Capability denials (\(length)):**\n"
             + (map("- `" + (.command // .path // .target // "?") + "`") | join("\n"))
        end
    ' 2>/dev/null
  )

  net_blocks=$(
    echo "${audit_json}" | jq -r '
      [ .network_events[]?
        | select(.blocked == true or .status == 403 or .action == "deny")
      ]
      | if length == 0 then empty
        else "**Network blocks (\(length)):**\n"
             + (map("- `" + (.host // .url // "?") + "`") | join("\n"))
        end
    ' 2>/dev/null
  )

  [[ -z "${denials:-}" && -z "${net_blocks:-}" ]] && return

  local body=""
  [[ -n "${denials:-}" ]]    && body+="${denials}"
  [[ -n "${net_blocks:-}" ]] && body+=$'\n\n'"${net_blocks}"

  buildkite-agent annotate \
    "### 🛡️ nono sandbox — blocked calls (session \`${session_id}\`)

${body}" \
    --context "nono-sandbox-summary" --style "warning" 2>/dev/null || true
}
_nono_audit_annotation

# ── Final annotation ───────────────────────────────────────────────────────────
if [[ $PI_EXIT -eq 0 ]]; then
  buildkite-agent annotate \
    "### ✅ pi-agent completed

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}" \
    --context "pi-agent-progress" --style "success"
else
  buildkite-agent annotate \
    "### ❌ pi-agent failed (exit ${PI_EXIT})

**Workflow:** \`${WORKFLOW}\`
**Ref:** ${REF}
See the job log for details." \
    --context "pi-agent-progress" --style "error"
fi

exit $PI_EXIT
