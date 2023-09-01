#!/bin/bash

set -euo pipefail

echo "steps:"

source .buildkite/scripts/branches.sh

for BRANCH in "${BRANCHES[@]}"; do
  cat <<EOF
  - trigger: elasticsearch-dra-workflow
    label: Trigger DRA snapshot workflow for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      env:
        DRA_WORKFLOW: snapshot
EOF

	# Don't trigger staging workflow for main branch
	if [[ "$BRANCH" != "main" ]]; then
		cat <<EOF
  - trigger: elasticsearch-dra-workflow
    label: Trigger DRA staging workflow for $BRANCH
    async: true
    build:
      branch: "$BRANCH"
      env:
        DRA_WORKFLOW: staging
EOF
	fi
done
