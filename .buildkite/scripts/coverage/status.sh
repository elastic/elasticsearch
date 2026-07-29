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

SLUG="${BUILDKITE_REPO_SLUG:-elastic/elasticsearch}"
URL="${BUILDKITE_BUILD_URL:-}"
[[ -n "${BUILDKITE_JOB_ID:-}" && -n "$URL" ]] && URL="$URL#${BUILDKITE_JOB_ID}"

gh api --method POST "repos/$SLUG/statuses/$BUILDKITE_COMMIT" \
  -f state="$STATE" \
  -f context="coverage/$NAME" \
  -f description="${DESCRIPTION:0:139}" \
  -f target_url="$URL" >/dev/null 2>&1 \
  && echo "--- status coverage/$NAME -> $STATE" \
  || echo "--- could not post status coverage/$NAME (non-fatal)"

exit 0
