#!/bin/bash

set -euo pipefail

echo --- Installing bun
npm install -g bun

echo --- Generating pipeline
bun .buildkite/scripts/pull-request/pipeline.generate.ts
