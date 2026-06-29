#!/bin/bash

set -euo pipefail

echo --- Generating pipeline

buildkite-agent pipeline upload << 'EOF'
steps:
  - label: "🤖 pi-agent analysis"
    command: .buildkite/scripts/pull-request/run-pi-agent.sh
    agents:
      provider: gcp
      image: family/elasticsearch-ubuntu-2404
      machineType: n2-standard-8
    timeout_in_minutes: 60
EOF
