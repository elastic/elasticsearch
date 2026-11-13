#!/bin/bash

set -euo pipefail

# Snapshot version is only known after upload-snapshot.sh; interpolate it into a
# trigger step at runtime (static YAML cannot see Buildkite meta-data).
VERSION=$(buildkite-agent meta-data get lucene-snapshot-version)
SHA=$(buildkite-agent meta-data get lucene-snapshot-sha)

if [[ -z "${TARGET_BRANCH:-}" ]]; then
  echo "Error: TARGET_BRANCH is required to trigger lucene-snapshot-update"
  exit 1
fi

cat <<EOF | buildkite-agent pipeline upload
steps:
  - trigger: "elasticsearch-agentic-workflow"
    label: ":robot_face: lucene-snapshot-update"
    async: false
    build:
      message: "lucene-snapshot-update: ${VERSION} → ${TARGET_BRANCH}"
      branch: "main"
      env:
        WORKFLOW: "lucene-snapshot-update"
        TARGET_BRANCH: "${TARGET_BRANCH}"
        LUCENE_SNAPSHOT_VERSION: "${VERSION}"
        LUCENE_SHA: "${SHA}"
        ARCHIMEDES_AGENT_MACHINE_TYPE: "n4-standard-16"
EOF
