#!/bin/bash
# .buildkite/scripts/bootstrap-estc.sh
#
# Bootstrap the headless-mode estc CLI (elastic/estc, broker-impl branch or a
# custom ESTC_REF) and start the credential broker outside the archimedes
# sandbox. All estc service tokens that would otherwise land in the agent's
# environment (buildkite, gradle-enterprise, …) are handed to the broker via
# stdin JSON and scrubbed from the environment before archimedes runs; the
# agent reaches them via ESTC_BROKER_SOCKET, never sees the token bytes.
#
# Design notes:
#   - Sourced from .buildkite/hooks/pre-command inside the USE_ARCHIMEDES=true /
#     ARCHIMEDES_USE_ESTC=true block, so it inherits vault_with_retry and the
#     already-resolved OPENROUTER_API_KEY / CURSOR_ACCESS_TOKEN / ES_DELIVERY_*
#     env vars from the caller. Only the *service* tokens (buildkite ro,
#     develocity) are consumed here and moved into the broker heap.
#   - Go 1.26 toolchain is not present on family/elasticsearch-ubuntu-2404; we
#     download and cache it under $HOME/.local/go-<version>. Both the toolchain
#     and the built estc binary are cache-keyed so warm agents skip the work.
#   - Broker stdin is held open via a shell FD kept alive by the job shell.
#     When the step finishes and the job shell exits, the FD closes → broker
#     sees EOF → exits cleanly. No post-command cleanup required.
#
# Contract (env in):
#   ESTC_REF                    - branch / tag / commit SHA of elastic/estc.
#                                 Defaults to "broker-impl" until merged;
#                                 override on the build trigger to test a ref.
#   GH_TOKEN                    - required, used to fetch estc source
#   BUILDKITE_RO_API_TOKEN      - required, exported as ESTC_TOKEN_BUILDKITE
#   DEVELOCITY_API_KEY          - required, exported as ESTC_TOKEN_GRADLE_ENTERPRISE
#
# Contract (env out):
#   ESTC_TOKEN_BUILDKITE        - for archimedes' spawnEstcBroker() (scrubbed
#   ESTC_TOKEN_GRADLE_ENTERPRISE  post-sandbox by archimedes' applySandbox())
#   ESTC_NONINTERACTIVE=1       - forbid daemon/browser/prompts inside estc
#   ESTC_OUTPUT_MODE=agent      - deterministic pipe-delimited output
#   CI=true                     - belt + braces for estc's own non-interactive
#                                 detection
#   PATH                        - $HOME/.local/bin prepended (estc, go)
#   BUILDKITE_RO_API_TOKEN, BUILDKITE_API_TOKEN, DEVELOCITY_API_KEY  - unset

set -euo pipefail

: "${GH_TOKEN:?bootstrap-estc: GH_TOKEN must be set before sourcing this script}"

ESTC_REPO="${ESTC_REPO:-elastic/estc}"
ESTC_REF="${ESTC_REF:-broker-impl}"

# ── 1. Resolve ESTC_REF to a stable SHA so caches are content-addressed ─────
#      Branch names move; the build cache must not.
if ! ESTC_SHA=$(gh api "repos/${ESTC_REPO}/commits/${ESTC_REF}" --jq '.sha' 2>/dev/null); then
  echo "bootstrap-estc: cannot resolve ${ESTC_REPO}@${ESTC_REF} to a SHA" >&2
  exit 1
fi
ESTC_SHA_SHORT="${ESTC_SHA:0:12}"
echo "--- Bootstrapping estc from ${ESTC_REPO}@${ESTC_REF} (${ESTC_SHA_SHORT})"

# ── 2. Cache: skip source fetch + build if we already have this SHA ─────────
ESTC_BIN_DIR="${HOME}/.local/estc-bin"
ESTC_BIN="${ESTC_BIN_DIR}/estc-${ESTC_SHA_SHORT}"
mkdir -p "${ESTC_BIN_DIR}" "${HOME}/.local/bin"

if [[ ! -x "${ESTC_BIN}" ]]; then
  # ── 3. Fetch source ──────────────────────────────────────────────────────
  ESTC_SRC="${HOME}/.local/estc-src/${ESTC_SHA_SHORT}"
  rm -rf "${ESTC_SRC}" && mkdir -p "${ESTC_SRC}"
  gh api "repos/${ESTC_REPO}/tarball/${ESTC_SHA}" \
    -H "Accept: application/vnd.github.raw+json" \
    | tar -xz -C "${ESTC_SRC}" --strip-components=1

  # ── 4. Toolchain: parse required Go version from go.work, cache per version ─
  GO_VERSION=$(awk '/^go [0-9]+\.[0-9]+/ {print $2; exit}' "${ESTC_SRC}/go.work")
  if [[ -z "${GO_VERSION}" ]]; then
    echo "bootstrap-estc: could not parse Go version from ${ESTC_SRC}/go.work" >&2
    exit 1
  fi
  GO_ROOT="${HOME}/.local/go-${GO_VERSION}"
  if [[ ! -x "${GO_ROOT}/bin/go" ]]; then
    echo "--- Installing Go ${GO_VERSION} (one-time per agent)"
    GO_TARBALL="/tmp/go-${GO_VERSION}.tar.gz"
    # go.dev/dl redirects the .tar.gz to dl.google.com but does NOT redirect the
    # .sha256 file — it returns HTML instead. Use dl.google.com directly for both
    # so the checksum comparison doesn't compare a hash against an HTML page.
    GO_DL_BASE="https://dl.google.com/go/go${GO_VERSION}.linux-amd64"
    curl -fsSL "${GO_DL_BASE}.tar.gz" -o "${GO_TARBALL}"
    curl -fsSL "${GO_DL_BASE}.tar.gz.sha256" -o "${GO_TARBALL}.sha256"
    EXPECTED=$(awk '{print tolower($1)}' "${GO_TARBALL}.sha256")
    ACTUAL=$(sha256sum "${GO_TARBALL}" | awk '{print $1}')
    if [[ "${EXPECTED}" != "${ACTUAL}" ]]; then
      echo "bootstrap-estc: Go tarball checksum mismatch (want ${EXPECTED} got ${ACTUAL})" >&2
      rm -f "${GO_TARBALL}" "${GO_TARBALL}.sha256"
      exit 1
    fi
    rm -rf "${GO_ROOT}" && mkdir -p "${GO_ROOT}"
    tar -xzf "${GO_TARBALL}" -C "${GO_ROOT}" --strip-components=1
    rm -f "${GO_TARBALL}" "${GO_TARBALL}.sha256"
  fi
  export PATH="${GO_ROOT}/bin:${PATH}"

  # ── 5. Build via estc's own release script; --local produces a single
  #      dist/<goos>-<goarch>/estc binary and runs bundleconfig so the embedded
  #      skills bundle is present (raw `go build ./cli/estc` would skip it).
  echo "--- Building estc (${ESTC_SHA_SHORT})"
  # Pre-set TAG so build-release.sh doesn't try `git rev-parse` — the
  # extracted source tarball has no .git directory.
  ( cd "${ESTC_SRC}" && TAG="$(date +%Y%m%d)-${ESTC_SHA_SHORT}" bash scripts/build-release.sh --local )
  install -m 0755 "${ESTC_SRC}/dist/platform/linux_amd64/bin/estc" "${ESTC_BIN}"
  rm -rf "${ESTC_SRC}"
fi

ln -sf "${ESTC_BIN}" "${HOME}/.local/bin/estc"
export PATH="${HOME}/.local/bin:${PATH}"
estc version show || true

# ── 6. Export ESTC_TOKEN_* vars for archimedes' spawnEstcBroker() ───────────
# archimedes (src/estc-broker.js) spawns the broker as its OWN child process
# before applying the Landlock sandbox, then keeps the broker alive via stdin
# pipe for the duration of the session. Starting the broker here (in the
# pre-command hook) would tie its lifetime to the hook's (sub)shell — when the
# hook exits, the FIFO write-end closes, the broker sees EOF, and dies before
# archimedes even runs.
#
# Instead, we export ESTC_TOKEN_* vars. archimedes' spawnEstcBroker() picks
# them up, starts the broker, and scrubSecrets() (called inside applySandbox())
# removes them from the environment before the sandboxed agent session begins.
: "${BUILDKITE_RO_API_TOKEN:?bootstrap-estc: BUILDKITE_RO_API_TOKEN must be set before sourcing this script}"
: "${DEVELOCITY_API_KEY:?bootstrap-estc: DEVELOCITY_API_KEY must be set before sourcing this script}"

export ESTC_TOKEN_BUILDKITE="${BUILDKITE_RO_API_TOKEN}"
export ESTC_TOKEN_GRADLE_ENTERPRISE="${DEVELOCITY_API_KEY}"
# Unset raw vars now — ESTC_TOKEN_* carry the same values and will be scrubbed
# by archimedes' applySandbox(). DEVELOCITY_ACCESS_KEY (the Gradle build-cache
# format string) is unrelated and must be left alone.
unset BUILDKITE_RO_API_TOKEN BUILDKITE_API_TOKEN DEVELOCITY_API_KEY

export ESTC_NONINTERACTIVE=1
export ESTC_OUTPUT_MODE=agent
export CI=true

echo "estc tokens exported (broker will be spawned by archimedes pre-sandbox)"

# ── 8. Sync estc skills into the checkout so the agent can use them. ────────
# --dir bypasses the settings-based project scan and syncs directly to the
# checkout directory. This works in headless CI where no settings.yaml exists.
echo "--- Syncing estc skills to ${BUILDKITE_BUILD_CHECKOUT_PATH:-${PWD}}"
estc sync --dir "${BUILDKITE_BUILD_CHECKOUT_PATH:-${PWD}}" \
          --plugins es-oncall,es-dev,estc \
          --verbose
