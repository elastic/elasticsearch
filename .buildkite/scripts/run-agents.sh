#!/bin/bash

set -euo pipefail

AGENT_WORKSPACE="$(cd .. && pwd)/agent-workspace"
mkdir -p "$AGENT_WORKSPACE"

cp -R .buildkite/scripts/run-agents/hooks "$AGENT_WORKSPACE/"

cd "$AGENT_WORKSPACE"

cp "$BUILDKITE_CONFIG_PATH" .
CONFIG_FILE="$(basename "$BUILDKITE_CONFIG_PATH")"

cat <<EOF >> "$CONFIG_FILE"
build-path=$AGENT_WORKSPACE/builds
disconnect-after-idle-timeout=600
hooks-path=$AGENT_WORKSPACE/hooks
disconnect-after-job=false
cancel-grace-period=300
tags="queue=elasticsearch-quick-agent"
EOF

buildkite-agent start --spawn-per-cpu 1 --config "$CONFIG_FILE"
