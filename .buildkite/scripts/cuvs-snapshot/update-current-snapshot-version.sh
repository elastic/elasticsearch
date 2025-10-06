#!/bin/bash

set -euo pipefail

SNAPSHOT_VERSION_FILE=.buildkite/scripts/cuvs-snapshot/current-snapshot-version
BRANCH_TO_UPDATE="${BRANCH_TO_UPDATE:-${BUILDKITE_BRANCH:-cuvs-snapshot}}"

if [[ -z "${CUVS_SNAPSHOT_VERSION:-}" ]]; then
  echo "CUVS_SNAPSHOT_VERSION not set. Set this to update the current snapshot version."
  exit 1
fi

if [[ "$CUVS_SNAPSHOT_VERSION" == "$(cat $SNAPSHOT_VERSION_FILE)" ]]; then
  echo "Current snapshot version already set to '$CUVS_SNAPSHOT_VERSION'. No need to update."
  exit 0
fi

echo "--- Configuring libcuvs/cuvs-java"
source .buildkite/scripts/cuvs-snapshot/configure.sh

if [[ "${SKIP_TESTING:-}" != "true" ]]; then
  echo "--- Testing snapshot before updating"
  ./gradlew -Druntime.java=24 :x-pack:plugin:gpu:yamlRestTest -S
fi

echo "--- Updating snapshot"

echo "$CUVS_SNAPSHOT_VERSION" > "$SNAPSHOT_VERSION_FILE"

CURRENT_SHA="$(gh api "/repos/elastic/elasticsearch/contents/$SNAPSHOT_VERSION_FILE?ref=$BRANCH_TO_UPDATE" | jq -r .sha)" || true

gh api -X PUT "/repos/elastic/elasticsearch/contents/$SNAPSHOT_VERSION_FILE" \
  -f branch="$BRANCH_TO_UPDATE" \
  -f message="Update cuvs snapshot version to $CUVS_VERSION" \
  -f content="$(base64 -w 0 "$WORKSPACE/$SNAPSHOT_VERSION_FILE")" \
  -f sha="$CURRENT_SHA"
