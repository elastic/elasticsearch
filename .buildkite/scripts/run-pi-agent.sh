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

# ── Run pi-agent ───────────────────────────────────────────────────────────────
PI_EXIT=0
case "${WORKFLOW}" in
  test-analysis)         pi-agent analyze --issue-url "${ISSUE_URL}" || PI_EXIT=$? ;;
  pull-request-fix)      pi-agent fix-pr  --pr-url    "${PR_URL}"    || PI_EXIT=$? ;;
  pull-request-creation) pi-agent create  --issue-url "${ISSUE_URL}" || PI_EXIT=$? ;;
esac

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
