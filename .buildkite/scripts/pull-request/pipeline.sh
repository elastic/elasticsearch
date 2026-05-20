#!/bin/bash

set -euo pipefail

echo --- Installing node
cd .buildkite
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
export NVM_DIR="$HOME/.nvm"
source "$HOME/.nvm/nvm.sh" --install
nvm install
corepack enable pnpm
pnpm install
cd -

echo --- Generating pipeline
node .buildkite/scripts/pull-request/pipeline.generate.ts
