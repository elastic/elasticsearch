#!/bin/bash

set -euo pipefail

# Decide whether an elasticsearch commit is worth sending to the
# elasticsearch-serverless-validate-submodule pipeline. Prints "advance" or
# "skip" and exits 0.
#
# "skip" means the candidate is behind, or identical to, the commit
# elasticsearch-serverless main already points at. Triggering then would check
# the submodule out backwards and run the serverless tests against stale
# elasticsearch code, which fails tests that are fine on current code and gets
# them muted on serverless main.
#
# "advance" covers a genuine forward move, diverged histories, and every case
# where the comparison cannot be made at all. This is a best-effort trigger-side
# filter that avoids creating a serverless build we already know is pointless;
# elasticsearch-serverless guards the same condition authoritatively in its own
# guard step, so falling through here is safe.
#
# Callers feed stdout straight into a pipeline definition, so the decision is
# the only thing printed there and the reasoning goes to stderr. Always say
# which way it went and why, so a build log shows whether the check ran.
#
# This runs during pipeline upload on the default buildkite agent, which does
# not ship the gh CLI, so the GitHub REST API is queried with curl instead.
# Authentication uses GH_TOKEN/GITHUB_TOKEN, which buildkite already injects.
#
# Usage: serverless-submodule-advance-decision.sh <candidate_commit>

CANDIDATE="$1"

log() {
  echo "serverless-submodule-advance-decision: $*" >&2
}

# Query the GitHub REST API. curl -f makes an HTTP error a non-zero exit, and
# set -o pipefail propagates that through the jq filter the callers pipe into,
# so a failed call is caught by their `if !` guards. curl's own stderr reaches
# the build log, so the reason for a failed call is visible.
github_api() {
  curl -fsSL \
    -H "Authorization: Bearer ${GH_TOKEN:-${GITHUB_TOKEN:-}}" \
    -H "Accept: application/vnd.github+json" \
    -H "X-GitHub-Api-Version: 2022-11-28" \
    "https://api.github.com/$1"
}

# A failed call and an unexpected answer are told apart on purpose. Both advance,
# so neither shows up as a build failure, and a filter that has quietly stopped
# filtering looks exactly like one that has nothing to skip.
if ! CURRENT=$(github_api "repos/elastic/elasticsearch-serverless/contents/elasticsearch?ref=main" | jq -r '.sha'); then
  log "the api call for the elasticsearch-serverless submodule commit failed, see the curl error above; advancing ${CANDIDATE} without the check"
  echo "advance"
  exit 0
fi

if [[ ! "${CURRENT}" =~ ^[0-9a-f]{40}$ ]]; then
  log "the elasticsearch-serverless submodule commit came back as '${CURRENT}', which is not a commit sha; advancing ${CANDIDATE} without the check"
  echo "advance"
  exit 0
fi

# Order matters: comparing CURRENT...CANDIDATE makes "behind" mean "the
# candidate is behind the submodule". Reversing it would silently invert the
# guard.
if ! STATUS=$(github_api "repos/elastic/elasticsearch/compare/${CURRENT}...${CANDIDATE}" | jq -r '.status'); then
  log "the api call comparing ${CANDIDATE} against the serverless submodule ${CURRENT} failed, see the curl error above; advancing without the check"
  echo "advance"
  exit 0
fi

case "${STATUS}" in
  behind | identical)
    log "candidate ${CANDIDATE} is ${STATUS} relative to the serverless submodule ${CURRENT}: skipping"
    echo "skip"
    ;;
  ahead | diverged)
    log "candidate ${CANDIDATE} is ${STATUS} relative to the serverless submodule ${CURRENT}: advancing"
    echo "advance"
    ;;
  *)
    log "comparing ${CANDIDATE} against the serverless submodule ${CURRENT} returned an unexpected status '${STATUS}'; advancing without the check"
    echo "advance"
    ;;
esac
