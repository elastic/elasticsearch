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

# Node.js, pnpm (via corepack) and the packages in pnpm-lock.yaml are pre-warmed
# in the elasticsearch-* CI agent images (ci-agent-images
# vm-images/elasticsearch/scripts/node.sh + node-cache.sh). Prefer those caches
# and only go to the network for whatever is missing, with retries, so a short
# registry/CDN blip doesn't fail the pre-command hook.
# See https://github.com/elastic/elasticsearch-team/issues/5544
_setup_node_retry() {
  local attempt
  for attempt in 1 2 3; do
    if "$@"; then
      return 0
    fi
    if [[ $attempt -lt 3 ]]; then
      echo "'$*' failed (attempt ${attempt}/3), retrying in $((attempt * 10))s..." >&2
      sleep $((attempt * 10))
    fi
  done
  echo "'$*' failed after 3 attempts" >&2
  return 1
}

_install_nvm() {
  curl -fsSL https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.4/install.sh | bash
}

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
    # nvm is a shell function, so `command -v nvm` can't detect a baked install;
    # check for nvm.sh instead and only run the installer when it's missing.
    export NVM_DIR="$HOME/.nvm"
    if [[ ! -s "$NVM_DIR/nvm.sh" ]]; then
      _setup_node_retry _install_nvm
    fi
    # --no-use: sourcing nvm.sh otherwise fails under `set -e` when no Node.js
    # is installed yet; the version is selected explicitly below.
    # shellcheck source=/dev/null
    source "$NVM_DIR/nvm.sh" --no-use

    # Use an installed Node.js matching .nvmrc (e.g. baked into the image) and
    # only download one if none is installed. A plain `nvm install` resolves the
    # .nvmrc version against the newest remote release on every job.
    if ! nvm use > /dev/null 2>&1; then
      echo "No installed Node.js matches .buildkite/.nvmrc ($(cat .nvmrc)); installing it"
      _setup_node_retry nvm install
    fi
    command -v node > /dev/null || { echo "ERROR: 'node' not on PATH after nvm install"; exit 1; }
  fi
fi

# pnpm install runs on all non-skip platforms, including Windows (where nvm is
# not used but node is pre-installed via the CI agent image).
if [[ "$SKIP_NODE_SETUP" != "true" ]]; then
  if ! command -v pnpm > /dev/null; then
    corepack enable pnpm
  fi
  # Retrying also covers corepack's first-use download of pnpm itself, which is
  # a single attempt with a 30s timeout.
  _setup_node_retry pnpm install --frozen-lockfile --prefer-offline
fi

unset -f _setup_node_retry _install_nvm

cd -
