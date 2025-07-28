#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/scripts/branches.sh

IS_FIRST=true
SKIP_DELAY="${SKIP_DELAY:-false}"

for BRANCH in "${BRANCHES[@]}"; do
  INTAKE_PIPELINE_SLUG="elasticsearch-intake"
  BUILD_JSON=$(curl -sH "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BRANCH}&state=passed&per_page=1" | jq '.[0] | {commit: .commit, url: .web_url}')
  LAST_GOOD_COMMIT=$(echo "${BUILD_JSON}" | jq -r '.commit')

  # Put a delay between each branch's set of pipelines by prepending each non-first branch with a sleep
  # This is to smooth out the spike in agent requests
  if [[ "$IS_FIRST" != "true" && "$SKIP_DELAY" != "true" ]]; then
      cat <<EOF
  - command: sleep 540
    soft_fail: true
  - wait: ~
    continue_on_failure: true
EOF
  fi
  IS_FIRST=false

  cat <<EOF
  - trigger: elasticsearch-periodic
    label: Trigger periodic pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
  - trigger: elasticsearch-periodic-packaging
    label: Trigger periodic-packaging pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
  - trigger: elasticsearch-periodic-platform-support
    label: Trigger periodic-platform-support pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
EOF
# Include forward compatibility tests only for the bugfix branch
if [[ "${BRANCH}" == "${BRANCHES[2]}" ]]; then
  cat <<EOF
  - trigger: elasticsearch-periodic-fwc
    label: Trigger periodic-fwc pipeline for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
EOF
fi
done
