#!/bin/bash

set -euo pipefail

echo --- Installing node
cd .buildkite
nvm install
npm install -g pnpm
pnpm install
cd -

echo --- Generating pipeline
node .buildkite/scripts/pull-request/pipeline.generate.ts
