#!/bin/bash

# Smart retry test filtering — see .buildkite/scripts/smart-retry/ for details.
# Reads: BUILDKITE_API_TOKEN, BUILDKITE_JOB_ID, BUILDKITE_PIPELINE_SLUG,
#         BUILDKITE_BUILD_NUMBER, ORIGIN_JOB_ID, TESTS_SEED
# Writes: .failed-test-history.json, buildkite-agent metadata and annotations
if ! command -v pnpm > /dev/null; then
  cd .buildkite
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
  export NVM_DIR="$HOME/.nvm"
  source "$HOME/.nvm/nvm.sh"
  nvm install
  corepack enable pnpm
  pnpm install
  cd -
fi

node .buildkite/scripts/smart-retry/main.ts
