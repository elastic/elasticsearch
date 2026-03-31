#!/bin/bash

set -euo pipefail

echo --- Installing bun
npm install -g bun@1.0.4

echo --- Generating pipeline
bun .buildkite/scripts/pull-request/pipeline.generate.ts
