#!/bin/bash

set -euo pipefail

# ── inject secrets ────────────────────────────────────────────────────
ELASTIC_LITELLM_API_KEY=$(vault read -field=litellm_token \
secret/ci/elastic-elasticsearch/agentic-workflows)
export ELASTIC_LITELLM_API_KEY

BUILDKITE_RO_API_TOKEN=$(vault read -field=buildkite_ro_token secret/ci/elastic-elasticsearch/agentic-workflows)
export BUILDKITE_RO_API_TOKEN

DEVELOCITY_API_KEY=$(vault read -field=develocity_api_token secret/ci/elastic-elasticsearch/agentic-workflows)
export DEVELOCITY_API_KEY

# ── bootstrap nono sandbox (cached after first run on this agent) ────────
NONO_BIN="${HOME}/.local/bin/nono"
if [[ ! -x "${NONO_BIN}" ]]; then
  echo "--- Installing nono sandbox CLI"
  NONO_VERSION=$(curl -fsSL -I https://github.com/always-further/nono/releases/latest \
    | grep -i "^location:" | grep -oP 'v\K[0-9a-zA-Z.-]+' | tr -d '\r')
  NONO_ARCH=$(dpkg --print-architecture)
  NONO_DEB="nono-cli_${NONO_VERSION}_${NONO_ARCH}.deb"
  NONO_TMP="$(mktemp -d)"
  curl -fsSL \
    "https://github.com/always-further/nono/releases/download/v${NONO_VERSION}/${NONO_DEB}" \
    -o "${NONO_TMP}/${NONO_DEB}"
  # Extract without requiring root — dpkg-deb unpacks the deb into a local dir
  dpkg-deb --extract "${NONO_TMP}/${NONO_DEB}" "${NONO_TMP}/nono-root"
  mkdir -p "${HOME}/.local/bin"
  cp "${NONO_TMP}/nono-root/usr/bin/nono" "${HOME}/.local/bin/nono"
  chmod +x "${HOME}/.local/bin/nono"
  rm -rf "${NONO_TMP}"
fi
export PATH="${HOME}/.local/bin:${PATH}"

# ── verify kernel sandbox support and pull pi profile pack ────────────
nono setup --check-only
nono pull always-further/pi

# ── bootstrap pi-agent (cached after first run on this agent) ─────────
PI_AGENT_DIR="${HOME}/.local/pi-agent"
if [[ ! -x "${PI_AGENT_DIR}/bin/pi-agent.js" ]]; then
# Use the gh_token from Vault (org-level access needed for the private release repo)
GH_ADMIN_TOKEN=$(vault read -field=gh_token secret/ci/elastic-elasticsearch/agentic-workflows)
PI_TARBALL_URL=$(curl -fsSL \
    -H "Authorization: Bearer ${GH_ADMIN_TOKEN}" \
    "https://api.github.com/repos/breskeby/rene-bk-experiments/releases" \
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

# ── run ───────────────────────────────────────────────────────────────
echo --- Running pi-agent analysis

# Emit a progress heartbeat every 5 minutes so the BK log doesn't go dark
# and the step doesn't appear hung. Killed automatically when pi-agent exits.
_heartbeat() {
  local i=0
  while sleep 300; do
    i=$((i + 1))
    echo "[pi-agent] still running... ${i}x5 min elapsed"
  done
}
_heartbeat &
_HEARTBEAT_PID=$!
trap 'kill "$_HEARTBEAT_PID" 2>/dev/null' EXIT

# Hard cap: kill the agent after 30 minutes so it can't silently burn the
# full 60-minute BK step timeout. If it hasn't finished by then it is
# either stuck or the task is too large for a single CI step.
timeout --signal=TERM --kill-after=60s 30m \
  nono run \
    --profile always-further/pi \
    --allow "${PI_AGENT_DIR}" \
    --allow-cwd \
    --silent \
    -- pi-agent analyze --issue-url "${ISSUE_URL}"
