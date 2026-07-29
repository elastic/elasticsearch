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
# Context is `coverage/<name>`, deliberately not the `elasticsearch-ci/` namespace - those belong
# to the existing integration, and the pipeline has
# `prevent_custom_statuses_from_using_buildkite_prefix` set.
#
# Never fails the caller. A step must not go red because a status could not be posted.
set -uo pipefail

NAME="${1:?usage: status.sh <name> <state> [description]}"
STATE="${2:?usage: status.sh <name> <state> [description]}"
DESCRIPTION="${3:-}"

[[ -z "${BUILDKITE:-}" ]] && exit 0
[[ -z "${BUILDKITE_COMMIT:-}" ]] && exit 0
command -v gh >/dev/null 2>&1 || { echo "gh unavailable - no status posted for $NAME"; exit 0; }

# GH_TOKEN is exported by hooks/pre-command from VAULT_GITHUB_TOKEN, which is only populated for
# steps that ask for it. Without a token gh is unauthenticated and cannot write a status, so fall
# back to the SAML-authorized admin token the agentic-workflows path carries.
if [[ -z "${GH_TOKEN:-}" ]] && command -v vault >/dev/null 2>&1; then
  GH_TOKEN=$(vault read -field=gh_admin_token secret/ci/elastic-elasticsearch/agentic-workflows 2>/dev/null) || true
  export GH_TOKEN
fi
if [[ -z "${GH_TOKEN:-}" ]]; then
  echo "--- no GH_TOKEN available - cannot post status coverage/$NAME"
  exit 0
fi

SLUG="${BUILDKITE_REPO_SLUG:-elastic/elasticsearch}"
URL="${BUILDKITE_BUILD_URL:-}"
[[ -n "${BUILDKITE_JOB_ID:-}" && -n "$URL" ]] && URL="$URL#${BUILDKITE_JOB_ID}"

gh api --method POST "repos/$SLUG/statuses/$BUILDKITE_COMMIT" \
  -f state="$STATE" \
  -f context="coverage/$NAME" \
  -f description="${DESCRIPTION:0:139}" \
  -f target_url="$URL" >/tmp/status-out 2>&1 \
  && echo "--- status coverage/$NAME -> $STATE" \
  || { echo "--- could not post status coverage/$NAME (non-fatal):"; head -3 /tmp/status-out; }

exit 0
