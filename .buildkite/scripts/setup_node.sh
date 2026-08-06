#!/bin/bash

cd .buildkite

# Node.js 24 requires glibc >= 2.25. Older Linux platforms tested in
# maintenance branches (e.g. RHEL 7, OracleLinux 7, SLES 12) ship with
# glibc 2.17; the node binary exits immediately with missing-symbol errors
# on those systems, which would abort the pre-command hook. Detect this
# early and skip node/pnpm setup so the packaging steps can still run
# (smart retry is simply unavailable on those platforms).
# Exported so smart-retry.sh can detect the declared skip vs an unexpected failure.
export SKIP_NODE_SETUP=false

# Don't do this part on Windows
if ! command -v choco > /dev/null; then
  if [[ "$(uname -s)" == "Linux" ]]; then
    # `|| true` is required: the hook runs under `set -euo pipefail`, so a
    # missing or non-zero-exiting ldd (musl, stripped images) would otherwise
    # abort the hook — the exact failure this check exists to avoid.
    _glibc=$( { ldd --version 2>/dev/null || true; } | awk 'NR==1{print $NF}')
    # Only compare when the last field really is a MAJOR.MINOR version: other
    # libc implementations and localized ldd output put arbitrary text there,
    # and arithmetic comparison of a non-number would abort the hook too.
    if [[ "$_glibc" =~ ^([0-9]+)\.([0-9]+) ]]; then
      _major=${BASH_REMATCH[1]}
      _minor=${BASH_REMATCH[2]}
      if [[ "$_major" -lt 2 ]] || [[ "$_major" -eq 2 && "$_minor" -lt 25 ]]; then
        echo "Skipping Node.js setup: glibc ${_glibc} < 2.25 (Node.js 24 requires glibc >= 2.25)"
        export SKIP_NODE_SETUP=true
      fi
    else
      # Unknown libc: attempt the install rather than silently disabling smart
      # retry. A genuinely unusable node still fails loudly below.
      echo "Could not determine glibc version from ldd output [${_glibc}]; attempting Node.js setup anyway"
    fi
    unset _glibc _major _minor
  fi

  if [[ "$SKIP_NODE_SETUP" != "true" ]]; then
    if ! command -v nvm > /dev/null; then
      curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
      export NVM_DIR="$HOME/.nvm"
      source "$HOME/.nvm/nvm.sh" --install
    fi

    nvm install
    command -v node > /dev/null || { echo "ERROR: 'node' not on PATH after nvm install"; exit 1; }
  fi
fi

# pnpm install runs on all non-skip platforms, including Windows (where nvm is
# not used but node is pre-installed via the CI agent image).
if [[ "$SKIP_NODE_SETUP" != "true" ]]; then
  if ! command -v pnpm > /dev/null; then
    corepack enable pnpm
  fi
  pnpm install
fi

cd -
