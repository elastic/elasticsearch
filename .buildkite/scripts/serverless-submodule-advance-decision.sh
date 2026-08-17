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
# gh authenticates via GH_TOKEN/GITHUB_TOKEN.
#
# Usage: serverless-submodule-advance-decision.sh <candidate_commit>

CANDIDATE="$1"

log() {
  echo "serverless-submodule-advance-decision: $*" >&2
}

CURRENT=$(gh api "repos/elastic/elasticsearch-serverless/contents/elasticsearch?ref=main" --jq '.sha' 2>/dev/null || true)

if [[ ! "${CURRENT}" =~ ^[0-9a-f]{40}$ ]]; then
  log "could not read the elasticsearch-serverless submodule commit; advancing ${CANDIDATE} without the check"
  echo "advance"
  exit 0
fi

# Order matters: comparing CURRENT...CANDIDATE makes "behind" mean "the
# candidate is behind the submodule". Reversing it would silently invert the
# guard.
STATUS=$(gh api "repos/elastic/elasticsearch/compare/${CURRENT}...${CANDIDATE}" --jq '.status' 2>/dev/null || true)

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
    log "could not compare ${CANDIDATE} against the serverless submodule ${CURRENT}; advancing without the check"
    echo "advance"
    ;;
esac
