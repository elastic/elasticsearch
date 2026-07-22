#!/bin/bash

cd .buildkite

# Don't do this part on Windows
if ! command -v choco > /dev/null; then
  if ! command -v nvm > /dev/null; then
    curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
    export NVM_DIR="$HOME/.nvm"
    source "$HOME/.nvm/nvm.sh" --install
  fi

  nvm install
fi

if ! command -v pnpm > /dev/null; then
  corepack enable pnpm
fi

pnpm install

cd -
