#!/bin/bash

set -euo pipefail

echo --- Generating pipeline

# ── inject secrets ────────────────────────────────────────────────────
ELASTIC_LITELLM_API_KEY=$(vault read -field=litellm_token \
secret/ci/elastic-elasticsearch/agentic-workflows)
export ELASTIC_LITELLM_API_KEY

BUILDKITE_RO_API_TOKEN=$(vault read -field=buildkite_ro_token secret/ci/elastic-elasticsearch/agentic-workflows)
export BUILDKITE_RO_API_TOKEN

DEVELOCITY_API_KEY=$(vault read -field=develocity_api_token secret/ci/elastic-elasticsearch/agentic-workflows)
export DEVELOCITY_API_KEY

# ── bootstrap pi-agent (cached after first run on this agent) ─────────
PI_AGENT_DIR="${HOME}/.local/pi-agent"
if [[ ! -x "${PI_AGENT_DIR}/bin/pi-agent.js" ]]; then
# Use the org-level admin token (broader repo access) to fetch the private release
GH_ADMIN_TOKEN=$(vault read -field=develocity_api_token secret/ci/elastic-elasticsearch/agentic-workflows)
PI_TARBALL_URL=$(curl -fsSL \
    -H "Authorization: Bearer ${GH_ADMIN_TOKEN}" \
    "https://api.github.com/repos/elastic/rene-bk-experiments/releases" \
    | node -e "
        const rs = JSON.parse(require('fs').readFileSync('/dev/stdin','utf8'));
        const r  = rs.find(r => r.tag_name.startsWith('pi-agent-'));
        const a  = r.assets.find(a => a.name.endsWith('.tgz') && !a.name.includes('sha256'));
        process.stdout.write(a.url);
    ")
TMPDIR_DL="$(mktemp -d)"
curl -fsSL \
    -H "Authorization: Bearer ${GH_ADMIN_TOKEN}" \
    -H "Accept: application/octet-stream" \
    "${PI_TARBALL_URL}" -o "${TMPDIR_DL}/pi-agent.tgz"
rm -rf "${PI_AGENT_DIR}" && mkdir -p "${PI_AGENT_DIR}"
tar -xzf "${TMPDIR_DL}/pi-agent.tgz" -C "${PI_AGENT_DIR}" --strip-components=1
npm install --prefix "${PI_AGENT_DIR}" --omit=dev --no-audit --no-fund --silent
mkdir -p "${HOME}/.local/bin"
ln -sf "${PI_AGENT_DIR}/bin/pi-agent.js" "${HOME}/.local/bin/pi-agent"
chmod +x "${PI_AGENT_DIR}/bin/pi-agent.js"
rm -rf "${TMPDIR_DL}"
fi
export PATH="${HOME}/.local/bin:${PATH}"

# ── run ───────────────────────────────────────────────────────────────
echo --- Running pi-agent analysis

pi-agent analyze --issue-url "${ISSUE_URL}"

# node .buildkite/scripts/pull-request/pipeline.generate.ts
