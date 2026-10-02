#!/bin/bash

# archimedes-env.sh — validate the image-baked Node.js + archimedes runtime and
# provision secrets for the elasticsearch-agentic-workflow pipeline.
#
# Sourced (not executed) from .buildkite/hooks/pre-command when
# USE_ARCHIMEDES=true, so the exports below land in the job's environment.
#
# The pre-command hook has already activated the preinstalled Node.js and added
# $HOME/.local/bin to PATH before sourcing this file. This script verifies that
# baked runtime and then injects the workflow's secrets. It does no runtime
# bootstrap or network install.
#
# Requires: vault_with_retry() from the calling hook.

set -euo pipefail

# Defense-in-depth pipeline gate: only the agentic-workflow pipeline may read
# the agentic-workflows secrets. Any other pipeline setting USE_ARCHIMEDES=true
# fails again here before the first agentic-workflows vault access.
# https://github.com/elastic/elasticsearch/issues/159040
if [[ "${BUILDKITE_PIPELINE_SLUG:-}" != "elasticsearch-agentic-workflow" ]]; then
  echo "USE_ARCHIMEDES=true is only permitted on the elasticsearch-agentic-workflow pipeline" \
       "(got '${BUILDKITE_PIPELINE_SLUG:-<unset>}'); refusing to read agentic-workflow secrets" >&2
  exit 1
fi

if ! command -v node >/dev/null 2>&1; then
  echo "expected baked Node.js on PATH for the agentic workflow, but 'node' was not found" >&2
  exit 1
fi

if ! command -v archimedes >/dev/null 2>&1; then
  echo "expected baked archimedes on PATH at ${HOME}/.local/bin/archimedes, but it was not found" >&2
  exit 1
fi

# OpenRouter API key — the bundled models.json resolves it automatically.
OPENROUTER_API_KEY=$(vault_with_retry read -field=openrouter_token secret/ci/elastic-elasticsearch/agentic-workflows)
export OPENROUTER_API_KEY

# Cursor API key — the fallback when OpenRouter is not available.
CURSOR_ACCESS_TOKEN=$(vault_with_retry read -field=cursor_token secret/ci/elastic-elasticsearch/agentic-workflows)
export CURSOR_ACCESS_TOKEN

# Read-only Buildkite token for the agent's bk_* tools. The bundled
# buildkite-jobs extension reads BUILDKITE_API_TOKEN, so alias it here
# (scoped to this step's environment).
BUILDKITE_RO_API_TOKEN=$(vault_with_retry read -field=buildkite_ro_token secret/ci/elastic-elasticsearch/agentic-workflows)
export BUILDKITE_RO_API_TOKEN
export BUILDKITE_API_TOKEN="${BUILDKITE_RO_API_TOKEN}"

# Develocity API key for build-scan reads during analysis.
DEVELOCITY_API_KEY=$(vault_with_retry read -field=develocity_api_token secret/ci/elastic-elasticsearch/agentic-workflows)
export DEVELOCITY_API_KEY

# ES delivery stats for resolving test history AND publishing results.
ES_DELIVERY_STATS_URL=$(vault_with_retry read -field=es_delivery_stats_url secret/ci/elastic-elasticsearch/agentic-workflows)
export ES_DELIVERY_STATS_URL

ES_DELIVERY_STATS_API_KEY=$(vault_with_retry read -field=es_delivery_stats_api_key secret/ci/elastic-elasticsearch/agentic-workflows)
export ES_DELIVERY_STATS_API_KEY

# gh_admin_token is SAML-authorized for the elastic org. The build's default
# GH_TOKEN is not, and returns 403 on elastic-org API reads. gh reads GH_TOKEN,
# so export it here before archimedes uses gh at runtime for issue comment/edit.
GH_ADMIN_TOKEN=$(vault_with_retry read -field=gh_admin_token secret/ci/elastic-elasticsearch/agentic-workflows)
export GH_TOKEN="${GH_ADMIN_TOKEN}"
unset GH_ADMIN_TOKEN

echo "--- Using baked node $(node --version) and archimedes $(archimedes version)"

# No external nono CLI bootstrap is needed: archimedes sandboxes itself in-process
# via the bundled nono-ts SDK, applied automatically by the `archimedes ci`
# entry point.
