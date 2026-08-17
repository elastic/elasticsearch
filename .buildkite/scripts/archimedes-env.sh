#!/bin/bash

# archimedes-env.sh — provision secrets and resolve the archimedes CLI for the
# elasticsearch-agentic-workflow pipeline.
#
# Sourced (not executed) from .buildkite/hooks/pre-command when
# USE_ARCHIMEDES=true, so the exports below land in the job's environment.
# Completely inert for every other CI step.
#
# archimedes is the self-contained agentic CI runner from
# https://github.com/elastic/elasticsearch-infra (archimedes/). It is baked into
# the elasticsearch-* agent images by
# ci-agent-images/vm-images/elasticsearch/scripts/archimedes.sh.
#
# Version selection — ARCHIMEDES_VERSION:
#   unset     use whatever is installed (baked into the image). No network.
#   latest    resolve the newest release and install it if it differs.
#   <x.y.z>   install that exact release if it differs. For reproducible re-runs
#             of an older build.
#
# Requires: vault_with_retry() from the calling hook.

set -euo pipefail

ARCHIMEDES_REPO="${ARCHIMEDES_REPO:-elastic/elasticsearch-infra}"

# ── Secrets ───────────────────────────────────────────────────────────────────
# All live at secret/ci/elastic-elasticsearch/agentic-workflows.

# OpenRouter API key — the bundled models.json resolves it automatically.
OPENROUTER_API_KEY=$(vault_with_retry read -field=openrouter_token secret/ci/elastic-elasticsearch/agentic-workflows)
export OPENROUTER_API_KEY

# Cursor API key — the fallback provider when OpenRouter is unavailable.
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
# so export it before resolving any release: the installer uses gh to fetch the
# release, and archimedes uses gh at runtime for issue comment/edit.
GH_ADMIN_TOKEN=$(vault_with_retry read -field=gh_admin_token secret/ci/elastic-elasticsearch/agentic-workflows)
export GH_TOKEN="${GH_ADMIN_TOKEN}"
unset GH_ADMIN_TOKEN

# The baked install symlinks the CLI here.
export PATH="${HOME}/.local/bin:${PATH}"

# ── Resolve the CLI ───────────────────────────────────────────────────────────

_archimedes_installed=$(archimedes version 2>/dev/null || echo "")
_archimedes_requested="${ARCHIMEDES_VERSION:-}"
_archimedes_provenance=""
_archimedes_target=""

# Resolve the newest release tag, or empty when it cannot be determined.
_archimedes_resolve_latest() {
  local tag
  tag=$(gh api "repos/${ARCHIMEDES_REPO}/releases/latest" --jq '.tag_name' 2>/dev/null || true)
  if [[ -z "${tag}" ]] || [[ "${tag}" == "null" ]]; then
    # /releases/latest is absent when every release is a prerelease.
    tag=$(gh api "repos/${ARCHIMEDES_REPO}/releases" \
      --jq '[.[] | select(.tag_name | startswith("archimedes-"))] | sort_by(.created_at) | last | .tag_name' 2>/dev/null || true)
  fi
  [[ "${tag}" == "null" ]] && tag=""
  printf '%s' "${tag#archimedes-v}"
}

if [[ -z "${_archimedes_requested}" ]] && [[ -n "${_archimedes_installed}" ]]; then
  # Default: trust the image. No network, no install.
  _archimedes_provenance="baked into agent image"

elif [[ -z "${_archimedes_requested}" ]]; then
  # Nothing installed — an image predating the bake, or one where the bake was
  # skipped (glibc < 2.25). Fall back to latest rather than failing the build.
  echo "--- archimedes: nothing installed on this agent, falling back to latest"
  _archimedes_target=$(_archimedes_resolve_latest)
  _archimedes_provenance="latest (${_archimedes_target:-unresolved}) — no baked install found"

elif [[ "${_archimedes_requested}" == "latest" ]]; then
  _archimedes_target=$(_archimedes_resolve_latest)
  _archimedes_provenance="latest (${_archimedes_target:-unresolved})"

else
  _archimedes_target="${_archimedes_requested#archimedes-v}"
  _archimedes_target="${_archimedes_target#v}"
  _archimedes_provenance="pinned (${_archimedes_target})"
fi

if [[ -z "${_archimedes_target}" ]] && [[ -z "${_archimedes_installed}" ]]; then
  echo "archimedes: no install present and no release could be resolved from ${ARCHIMEDES_REPO}" >&2
  exit 1
fi

# Install only when the resolved target differs from what is already present.
if [[ -n "${_archimedes_target}" ]] && [[ "${_archimedes_target}" != "${_archimedes_installed}" ]]; then
  echo "--- Bootstrapping archimedes ${_archimedes_target} (installed: ${_archimedes_installed:-none})"
  # Fetch the installer from the DEFAULT BRANCH, not from the target's tag.
  # ARCHIMEDES_VERSION support was added to bootstrap.sh only recently, so the
  # copy committed at an older tag silently IGNORES the pin and installs the
  # newest release instead (verified: the v0.46.0-tagged bootstrap.sh has zero
  # mentions of ARCHIMEDES_VERSION). The default-branch installer honours it.
  if ! gh api "repos/${ARCHIMEDES_REPO}/contents/archimedes/bootstrap.sh" \
         -H "Accept: application/vnd.github.raw+json" \
       | ARCHIMEDES_VERSION="${_archimedes_target}" bash -s; then
    echo "archimedes bootstrap failed" >&2
    exit 1
  fi
fi

# Provenance is logged on EVERY run. With the default path using whatever is
# baked, the running version is otherwise invisible — and a stale agent image
# silently running old workflow code is exactly the failure this surfaces.
echo "--- archimedes $(archimedes version 2>/dev/null || echo '(unresolved)') — ${_archimedes_provenance}"

unset -f _archimedes_resolve_latest
unset _archimedes_installed _archimedes_requested _archimedes_provenance _archimedes_target

# No external nono CLI bootstrap is needed: archimedes sandboxes itself in-process
# via the bundled nono-ts SDK, applied automatically by the `archimedes ci` entry
# point.
