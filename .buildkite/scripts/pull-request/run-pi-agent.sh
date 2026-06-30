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
echo "--- nono: install"
NONO_BIN="${HOME}/.local/bin/nono"
if [[ ! -x "${NONO_BIN}" ]]; then
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
echo "--- nono: verify sandbox + pull pi profile"
nono setup --check-only
nono pull always-further/pi

# ── bootstrap pi-agent (cached after first run on this agent) ─────────
echo "--- pi-agent: install"
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

# ── configure pi project settings ───────────────────────────────────────
# .pi is gitignored, so write the project settings at runtime.
# httpIdleTimeoutMs: caps each LLM streaming connection at 60 s of idle.
# retry.*: 1 retry max so a hung call fails in ~2 min instead of ~23 min.
mkdir -p .pi
cat > .pi/settings.json << 'EOF'
{
  "httpIdleTimeoutMs": 60000,
  "retry": {
    "maxRetries": 1,
    "provider": {
      "maxRetries": 1,
      "maxRetryDelayMs": 10000
    }
  }
}
EOF

# ── run ───────────────────────────────────────────────────────────────
# +++ expands this section by default in the BK log so the pi-agent output
# is immediately visible without having to open a collapsed group.
echo "+++ pi-agent: analysis"

# Use nono wrap (not nono run) so nono exec(2)s directly into pi-agent.
# With nono run the supervisor PTY mux buffers the child's stdout; in a
# non-interactive CI environment that buffer is never flushed and all
# pi-agent output is lost if the process is killed by the outer timeout.
# nono wrap has no supervisor — nono disappears from the process tree and
# pi-agent's stdout/stderr go straight to the BK log.
#
# Hard cap: kill after 45 minutes. Exit 124 = timeout, 137 = SIGKILL.
timeout --signal=TERM --kill-after=60s 45m \
  nono wrap \
    -v \
    --profile always-further/pi \
    --allow "${PI_AGENT_DIR}" \
    --allow-cwd \
    -- pi-agent analyze --issue-url "${ISSUE_URL}" \
      --verbose \
      --append-system-prompt "IMPORTANT CI CONSTRAINTS: Do NOT use fetch_content or web_search to access gradle-enterprise.elastic.co, develocity.elastic.co, or any other Elastic-internal URLs that require SSO authentication. These connections hang indefinitely in CI because no browser-based SSO flow is possible. If you need build scan data, note that it is unavailable and continue the analysis using only GitHub issue content, code files, and other accessible sources."
