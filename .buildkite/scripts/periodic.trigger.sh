#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/scripts/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  cat <<EOF
  - trigger: elasticsearch-periodic
    label: Trigger periodic pipeline for $BRANCH
    async: true
    build:
      branch: $BRANCH
  - trigger: elasticsearch-periodic-packaging
    label: Trigger periodic-packaging pipeline for $BRANCH
    async: true
    build:
      branch: $BRANCH
  - trigger: elasticsearch-periodic-platform-support
    label: Trigger periodic-platform-support pipeline for $BRANCH
    async: true
    build:
      branch: $BRANCH
EOF
done
