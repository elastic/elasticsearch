#!/bin/bash

# Runs a flakiness check on a developer checkout: prepares what local.ts needs, then hands it every argument
# unchanged. Usage and behaviour are in the README's "Local CLI" section.

set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"

# Check Node.js version against the required major version in .buildkite/.nvmrc
required_major="$(< "$repo_root/.buildkite/.nvmrc")"
required_major="${required_major#v}"
required_major="${required_major%%.*}"
node_version="$(node --version)"
node_major="${node_version#v}"
node_major="${node_major%%.*}"
if [ "$node_major" -lt "$required_major" ]; then
  echo "local.sh: Node.js $required_major or newer is required (found $node_version); see .buildkite/.nvmrc." >&2
  exit 1
fi

# pnpm, because .buildkite/pnpm-lock.yaml is what CI installs from
if command -v pnpm > /dev/null; then
  pnpm=(pnpm)
else
  pnpm=(npx --yes pnpm@latest)
fi
(cd "$repo_root/.buildkite" && "${pnpm[@]}" install --frozen-lockfile)

exec node "$repo_root/.buildkite/scripts/flakiness-detection/entrypoints/local.ts" "$@"
