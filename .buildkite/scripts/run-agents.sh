#!/bin/bash

set -euo pipefail

AGENT_WORKSPACE="$(mktemp -d)"
mkdir -p "$AGENT_WORKSPACE"

cp -R .buildkite/scripts/run-agents/hooks "$AGENT_WORKSPACE/"

cd "$AGENT_WORKSPACE"

cp "$BUILDKITE_CONFIG_PATH" .
CONFIG_FILE="$(basename "$BUILDKITE_CONFIG_PATH")"

cat <<EOF > "$CONFIG_FILE"
build-path=$AGENT_WORKSPACE/builds
hooks-path=$AGENT_WORKSPACE/hooks
plugins-path=$AGENT_WORKSPACE/plugins
disconnect-after-idle-timeout=600
disconnect-after-job=false
cancel-grace-period=300
tags="queue=elasticsearch-quick-agent"
acquire-job=""
tags-from-gcp="false"
EOF

grep 'token=' "$BUILDKITE_CONFIG_PATH" >> "$CONFIG_FILE"

unset ${!BUILDKITE_*}

# buildkite-agent start --spawn-per-cpu 1 --config "$CONFIG_FILE"
buildkite-agent start --config "$CONFIG_FILE"
