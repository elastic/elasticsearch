#!/bin/bash

set -euo pipefail

echo --- Installing node
nvm install 24
npm install -g pnpm
pnpm install

echo --- Generating pipeline
node .buildkite/scripts/pull-request/pipeline.generate.ts
