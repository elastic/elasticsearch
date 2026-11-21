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

# lucene-snapshot-build is BUILD_AND_READ for everyone; Archimedes can push a
# green bump to TARGET_BRANCH. Reject anything that is not a snapshot branch,
# and keep values YAML-safe for the pipeline upload below.
if [[ ! "$TARGET_BRANCH" =~ ^lucene_snapshot[A-Za-z0-9._-]*$ ]]; then
  echo "Error: TARGET_BRANCH must match lucene_snapshot* (got ${TARGET_BRANCH})"
  exit 1
fi
if [[ ! "$VERSION" =~ ^[A-Za-z0-9._-]+$ ]]; then
  echo "Error: lucene-snapshot-version has unexpected characters: ${VERSION}"
  exit 1
fi
if [[ ! "$SHA" =~ ^[a-fA-F0-9]+$ ]]; then
  echo "Error: lucene-snapshot-sha has unexpected characters: ${SHA}"
  exit 1
fi

yaml_string() { jq -n --arg v "$1" '$v'; }

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
        TARGET_BRANCH: $(yaml_string "$TARGET_BRANCH")
        LUCENE_SNAPSHOT_VERSION: $(yaml_string "$VERSION")
        LUCENE_SHA: $(yaml_string "$SHA")
EOF
