#!/bin/bash

set -euo pipefail

echo --- Generating pipeline
node .buildkite/scripts/pull-request/pipeline.generate.ts
