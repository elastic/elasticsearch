#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/scripts/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  if [[ "$BRANCH" == "9.0" ]]; then
    export VERSION_QUALIFIER="rc1"
  fi

  INTAKE_PIPELINE_SLUG="elasticsearch-intake"
  BUILD_JSON=$(curl -sH "Authorization: Bearer ${BUILDKITE_API_TOKEN}" "https://api.buildkite.com/v2/organizations/elastic/pipelines/${INTAKE_PIPELINE_SLUG}/builds?branch=${BRANCH}&state=passed&per_page=1" | jq '.[0] | {commit: .commit, url: .web_url}')
  LAST_GOOD_COMMIT=$(echo "${BUILD_JSON}" | jq -r '.commit')

  cat <<EOF
  - trigger: elasticsearch-dra-workflow
    label: Trigger DRA staging workflow for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      commit: "$LAST_GOOD_COMMIT"
      env:
        DRA_WORKFLOW: staging
        VERSION_QUALIFIER: ${VERSION_QUALIFIER:-}
EOF
done
