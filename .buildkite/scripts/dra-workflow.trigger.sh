#!/bin/bash

set -euo pipefail

source .buildkite/scripts/branches.sh

# We use that filtering to keep different schedule for different branches
if [ -n "${INCLUDED_BRANCHES:-}" ]; then
  # If set, only trigger the pipeline for the specified branches
  IFS=',' read -r -a BRANCHES <<< "${INCLUDED_BRANCHES}"
elif [ -n "${EXCLUDED_BRANCHES:-}" ]; then
  # If set, listed branches will be excluded from the list of branches in branches.json
  IFS=',' read -r -a EXCLUDED_BRANCHES <<< "${EXCLUDED_BRANCHES}"
  FILTERED_BRANCHES=()
    for BRANCH in "${BRANCHES[@]}"; do
      EXCLUDE=false
      for EXCLUDED_BRANCH in "${EXCLUDED_BRANCHES[@]}"; do
        if [ "$BRANCH" == "$EXCLUDED_BRANCH" ]; then
          EXCLUDE=true
          break
        fi
      done
      if [ "$EXCLUDE" = false ]; then
        FILTERED_BRANCHES+=("$BRANCH")
      fi
    done
    BRANCHES=("${FILTERED_BRANCHES[@]}")
fi


echo "steps:"

for BRANCH in "${BRANCHES[@]}"; do
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
