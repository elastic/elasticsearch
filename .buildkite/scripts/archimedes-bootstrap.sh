#!/bin/bash

# archimedes-bootstrap.sh — security-hardened bootstrap of the archimedes CLI
# for the elasticsearch-agentic-workflow pipeline.
#
# This helper exists so the bootstrap trust policy is a small, auditable,
# testable unit rather than inline hook code. It repairs the trust-boundary
# inversion tracked in https://github.com/elastic/elasticsearch/issues/159040:
# previously the pre-command hook honored build-controlled ARCHIMEDES_REPO /
# ARCHIMEDES_REF vars and piped a GitHub contents-API response straight into
# bash, meaning anyone who could set build env vars could pick the code that
# ran with agentic-workflow credentials already in the environment.
#
# Policy enforced here (fail closed on every violation):
#   1. Only the elasticsearch-agentic-workflow pipeline may bootstrap.
#   2. The source repo is a constant; there is no build-time override.
#   3. Only the pinned, immutable release identifier below is installed.
#      Upgrades are PR-reviewed changes to these constants.
#   4. Build-provided bootstrap overrides (ARCHIMEDES_REF, ARCHIMEDES_VERSION,
#      ARCHIMEDES_REPO, ARCHIMEDES_ARCHIVE) are rejected, not ignored, so a
#      testing attempt fails loudly instead of silently running the pin.
#   5. bootstrap.sh is downloaded to a file and its sha256 is verified against
#      the pinned checksum before it is executed. The installer itself then
#      verifies the sha256 of the release tarball it downloads, so the whole
#      chain from this repo's pinned constants to the installed binary is
#      integrity-checked.
#
# A baked archimedes binary on the agent image remains the preferred end
# state; the version check below already treats a baked or warm install of
# the pinned version as satisfied, so baking requires no changes here.

set -euo pipefail

# ── Pinned identity of the archimedes install ────────────────────────────────
# To upgrade: bump the version, then recompute the checksum of the installer
# at the new tag:
#   gh api "repos/elastic/elasticsearch-infra/contents/archimedes/bootstrap.sh?ref=archimedes-v<VER>" \
#     -H "Accept: application/vnd.github.raw+json" | sha256sum
ARCHIMEDES_APPROVED_REPO="elastic/elasticsearch-infra"
ARCHIMEDES_PINNED_VERSION="0.50.5"
ARCHIMEDES_BOOTSTRAP_SHA256="e06b4155f5374933a11c294055c73df1b8c7ff82d659da238f21815360d28546"

ARCHIMEDES_RELEASE_TAG="archimedes-v${ARCHIMEDES_PINNED_VERSION}"

# ── Policy checks ─────────────────────────────────────────────────────────────

if [[ "${BUILDKITE_PIPELINE_SLUG:-}" != "elasticsearch-agentic-workflow" ]]; then
  echo "archimedes bootstrap is only permitted on the elasticsearch-agentic-workflow pipeline" \
       "(got '${BUILDKITE_PIPELINE_SLUG:-<unset>}'); refusing to bootstrap" >&2
  exit 1
fi

# Reject build-controlled bootstrap inputs outright. Testing unreleased
# archimedes changes must happen on a non-production pipeline that does not
# hold production credentials — see issue #159040.
for _var in ARCHIMEDES_REF ARCHIMEDES_VERSION ARCHIMEDES_REPO ARCHIMEDES_ARCHIVE; do
  if [[ -n "${!_var:-}" ]]; then
    echo "${_var} is not honored on this pipeline: archimedes is pinned to" \
         "${ARCHIMEDES_RELEASE_TAG} from ${ARCHIMEDES_APPROVED_REPO}." >&2
    echo "Upgrades require a reviewed change to .buildkite/scripts/archimedes-bootstrap.sh" \
         "(https://github.com/elastic/elasticsearch/issues/159040)." >&2
    exit 1
  fi
done

# ── Install (skipped when the pinned version is already present) ─────────────

_installed_ver=$(archimedes version 2>/dev/null || echo "none")
if [[ "${_installed_ver}" == "${ARCHIMEDES_PINNED_VERSION}" ]]; then
  echo "archimedes ${_installed_ver} already installed (pinned)"
  exit 0
fi

echo "--- Bootstrapping archimedes ${ARCHIMEDES_RELEASE_TAG} (installed: ${_installed_ver})"

_tmpdir=$(mktemp -d)
trap 'rm -rf "${_tmpdir}"' EXIT

# Fetch the installer at the pinned release tag — never piped to bash: it is
# written to disk and checksum-verified first, so a compromised or mutated tag
# cannot inject code past this point.
if ! gh api "repos/${ARCHIMEDES_APPROVED_REPO}/contents/archimedes/bootstrap.sh?ref=${ARCHIMEDES_RELEASE_TAG}" \
       -H "Accept: application/vnd.github.raw+json" > "${_tmpdir}/bootstrap.sh"; then
  echo "failed to download archimedes bootstrap.sh at ${ARCHIMEDES_RELEASE_TAG}" >&2
  exit 1
fi

_actual_sha=$(sha256sum "${_tmpdir}/bootstrap.sh" | awk '{print $1}')
if [[ "${_actual_sha}" != "${ARCHIMEDES_BOOTSTRAP_SHA256}" ]]; then
  echo "archimedes bootstrap.sh checksum mismatch — refusing to execute" >&2
  echo "  expected: ${ARCHIMEDES_BOOTSTRAP_SHA256}" >&2
  echo "  actual:   ${_actual_sha}" >&2
  exit 1
fi

# ARCHIMEDES_VERSION pins the installer to the exact release; the installer
# downloads the release tarball plus its .sha256 asset and verifies it before
# installing.
if ! ARCHIMEDES_VERSION="${ARCHIMEDES_PINNED_VERSION}" bash "${_tmpdir}/bootstrap.sh"; then
  echo "archimedes bootstrap of ${ARCHIMEDES_RELEASE_TAG} failed" >&2
  exit 1
fi
