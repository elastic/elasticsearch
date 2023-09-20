#!/bin/bash

# The stuff in here doesn't need to fail a build if it fails for some reason, it's just cosmetic
set +e

# Initializing these annotations with empty <details> tags will make them expandable/collapsible when appended to later

if [[ "${BUILDKITE_LABEL:-}" == ":pipeline: Pipeline upload" ]]; then
  cat << EOF | buildkite-agent annotate --context "gradle-build-scans" --style "info"
<details>

<summary>Gradle build scan links</summary>
EOF

  cat << EOF | buildkite-agent annotate --context "ctx-gobld-metrics" --style "info"
<details>

<summary>Agent information from gobld</summary>
EOF
fi
