#!/bin/bash
#
# Publish a GitHub commit status for one coverage step.
#
#   status.sh <name> pending|success|failure|error [description]
#
# Why this exists: Buildkite's commit-status integration publishes a status per job and names it
# from the step, but coverage steps do not appear on the PR through it. Rather than change how
# other pipelines are declared, or wait on an integration this repo does not configure, each
# coverage step publishes its own status. Fully self-contained: nothing outside these scripts
# changes, and the result is visible on the PR from the moment a leg starts.
#
# Context is `coverage/<name>`: a namespace of our own, so it can never collide with what the
# integrations publish - `elasticsearch-ci/...` belongs to the trigger system, and `buildkite/...`
# is reserved by the pipeline's `prevent_custom_statuses_from_using_buildkite_prefix` setting.
#
# Never fails the caller. A step must not go red because a status could not be posted.
set -uo pipefail

NAME="${1:?usage: status.sh <name> <state> [description]}"
STATE="${2:?usage: status.sh <name> <state> [description]}"
DESCRIPTION="${3:-}"

[[ -z "${BUILDKITE:-}" ]] && exit 0
[[ -z "${BUILDKITE_COMMIT:-}" ]] && exit 0
command -v gh >/dev/null 2>&1 || { echo "gh unavailable - no status posted for $NAME"; exit 0; }

# Statuses go on the base repo. Buildkite has no variable carrying an owner/repo slug, only the
# checkout URL - derive the slug from it, falling back to the base repo if the URL is exotic.
SLUG="${BUILDKITE_REPO:-}"   # git@github.com:owner/repo.git or https://github.com/owner/repo.git
SLUG="${SLUG##*github.com?}"
SLUG="${SLUG%.git}"
[[ "$SLUG" == ?*/?* && "$SLUG" != *:* && "$SLUG" != */*/* ]] || SLUG="elastic/elasticsearch"

URL="${BUILDKITE_BUILD_URL:-}"
[[ -n "${BUILDKITE_JOB_ID:-}" && -n "$URL" ]] && URL="$URL#${BUILDKITE_JOB_ID}"

OUT_FILE="$(mktemp /tmp/coverage-status.XXXXXX 2>/dev/null || echo "/tmp/coverage-status.$$")"

# Auth. The ambient GH_TOKEN (hooks/pre-command, from VAULT_GITHUB_TOKEN) is a GitHub App token
# without `statuses: write` - it returns 403 "Resource not accessible by integration". So try it,
# and on failure retry with the SAML-authorized token in Vault, which can write statuses.
#
# The retry is on failure rather than on an empty token: the ambient one is present but
# insufficient, so an emptiness check never fires. A retry with an empty vault read would be
# unauthenticated and would overwrite the first attempt's diagnostics, hence the -n guard.
post_status() {
  local args=(
    -f state="$STATE"
    -f context="coverage/$NAME"
    -f description="${DESCRIPTION:0:139}"
  )
  [[ -n "$URL" ]] && args+=(-f target_url="$URL")
  gh api --method POST "repos/$SLUG/statuses/$BUILDKITE_COMMIT" "${args[@]}" >"$OUT_FILE" 2>&1
}

if post_status; then
  echo "--- status coverage/$NAME -> $STATE"
elif command -v vault >/dev/null 2>&1 \
     && GH_TOKEN=$(vault read -field=gh_admin_token secret/ci/elastic-elasticsearch/agentic-workflows 2>/dev/null) \
     && [[ -n "$GH_TOKEN" ]] \
     && export GH_TOKEN && post_status; then
  echo "--- status coverage/$NAME -> $STATE (via vault token)"
else
  echo "--- could not post status coverage/$NAME (non-fatal):"
  head -3 "$OUT_FILE" 2>/dev/null
fi

rm -f "$OUT_FILE"
exit 0
