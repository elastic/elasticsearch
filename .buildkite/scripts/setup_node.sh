#!/bin/bash

cd .buildkite

if ! command -v nvm > /dev/null; then
  curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
  export NVM_DIR="$HOME/.nvm"
  source "$HOME/.nvm/nvm.sh" --install
fi

nvm install

if ! command -v pnpm > /dev/null; then
  corepack enable pnpm
fi

pnpm install

ls -alh node_modules/ # TODO debugging

cd -
