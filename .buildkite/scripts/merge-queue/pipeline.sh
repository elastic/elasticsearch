#!/bin/bash

set -euo pipefail

echo --- Generating pipeline
node .buildkite/scripts/merge-queue/pipeline.generate.ts
