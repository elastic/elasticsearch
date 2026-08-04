#!/bin/bash

cd .buildkite

# Don't do this part on Windows
if ! command -v choco > /dev/null; then
  # Node.js 24 requires glibc >= 2.25. Older Linux platforms tested in
  # maintenance branches (e.g. RHEL 7, OracleLinux 7, SLES 12) ship with
  # glibc 2.17; the node binary exits immediately with missing-symbol errors
  # on those systems, which would abort the pre-command hook. Detect this
  # early and skip node/pnpm setup so the packaging steps can still run
  # (smart retry is simply unavailable on those platforms).
  _skip_node=false
  if [[ "$(uname -s)" == "Linux" ]]; then
    _glibc=$(ldd --version 2>/dev/null | awk 'NR==1{print $NF}')
    _major=${_glibc%%.*}
    _minor=${_glibc##*.}
    if [[ "$_major" -lt 2 ]] || [[ "$_major" -eq 2 && "$_minor" -lt 25 ]]; then
      echo "Skipping Node.js setup: glibc ${_glibc} < 2.25 (Node.js 24 requires glibc >= 2.25)"
      _skip_node=true
    fi
    unset _glibc _major _minor
  fi

  if [[ "$_skip_node" != "true" ]]; then
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
  fi

  unset _skip_node
fi

cd -
