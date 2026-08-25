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
#   BUILDKITE_RO_API_TOKEN      - required, moves into the broker as `buildkite`
#   DEVELOCITY_API_KEY          - required, moves into the broker as
#                                 `gradle-enterprise`
#
# Contract (env out):
#   ESTC_BROKER_SOCKET          - UDS path the sandboxed agent uses
#   ESTC_NONINTERACTIVE=1       - forbid daemon/browser/prompts inside estc
#   ESTC_OUTPUT_MODE=agent      - deterministic pipe-delimited output
#   CI=true                     - belt + braces for estc's own non-interactive
#                                 detection
#   PATH                        - $HOME/.local/bin prepended (estc, go)
#   BUILDKITE_RO_API_TOKEN, DEVELOCITY_API_KEY  - unset (moved into broker)

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
estc --version || true

# ── 6. Broker: assemble token JSON, spawn broker, wait for READY ────────────
: "${BUILDKITE_RO_API_TOKEN:?bootstrap-estc: BUILDKITE_RO_API_TOKEN must be set before sourcing this script}"
: "${DEVELOCITY_API_KEY:?bootstrap-estc: DEVELOCITY_API_KEY must be set before sourcing this script}"

ESTC_BROKER_DIR="${BUILDKITE_BUILD_CHECKOUT_PATH:-${PWD}}/.estc-broker"
mkdir -p "${ESTC_BROKER_DIR}"
chmod 700 "${ESTC_BROKER_DIR}"
ESTC_BROKER_SOCKET="${ESTC_BROKER_DIR}/broker.sock"
ESTC_BROKER_STDERR="${ESTC_BROKER_DIR}/broker.stderr.log"
ESTC_BROKER_TOKEN_FIFO="${ESTC_BROKER_DIR}/broker.stdin"

rm -f "${ESTC_BROKER_SOCKET}" "${ESTC_BROKER_TOKEN_FIFO}"
mkfifo -m 600 "${ESTC_BROKER_TOKEN_FIFO}"

# Broker credKey names come from estc/README §"Credential broker" — the
# scheme table (Bearer vs ApiKey) is enforced by the service registry.
# Feed via jq -Rs to escape token characters safely; never let a token
# reach a shell word.
BROKER_TOKENS_JSON=$(jq -n \
  --arg bk_token "${BUILDKITE_RO_API_TOKEN}" \
  --arg ge_token "${DEVELOCITY_API_KEY}" \
  '{tokens: {"buildkite": $bk_token, "gradle-enterprise": $ge_token}}')

# Spawn broker with stdin from the fifo. Hold the fifo's write end open on FD
# 9 in *this* shell (the job shell that also runs the step command); when the
# job finishes and this shell exits, FD 9 closes, broker sees stdin EOF, and
# exits cleanly. No post-command cleanup required.
estc broker serve --socket "${ESTC_BROKER_SOCKET}" \
    < "${ESTC_BROKER_TOKEN_FIFO}" \
    > "${ESTC_BROKER_DIR}/broker.stdout.log" \
    2> "${ESTC_BROKER_STDERR}" &
ESTC_BROKER_PID=$!
exec 9>"${ESTC_BROKER_TOKEN_FIFO}"
printf '%s' "${BROKER_TOKENS_JSON}" >&9
# Keep FD 9 open — do NOT close it. Broker drains stdin and exits on EOF.

# Wait for READY (broker prints it after socket bind + chmod 0600).
_estc_broker_ready=""
for _ in $(seq 1 50); do  # 50 * 200ms = 10s
  if grep -q '^READY$' "${ESTC_BROKER_DIR}/broker.stdout.log" 2>/dev/null; then
    _estc_broker_ready=1
    break
  fi
  if ! kill -0 "${ESTC_BROKER_PID}" 2>/dev/null; then
    echo "bootstrap-estc: broker exited before READY. stderr follows:" >&2
    cat "${ESTC_BROKER_STDERR}" >&2 || true
    exit 1
  fi
  sleep 0.2
done
if [[ -z "${_estc_broker_ready}" ]]; then
  echo "bootstrap-estc: timed out waiting for broker READY. stderr follows:" >&2
  cat "${ESTC_BROKER_STDERR}" >&2 || true
  kill "${ESTC_BROKER_PID}" 2>/dev/null || true
  exit 1
fi
unset _estc_broker_ready BROKER_TOKENS_JSON

# ── 7. Scrub the raw service tokens: the broker heap owns them now. ─────────
#      Left in place: OPENROUTER_API_KEY (pi provider auth, pinned+scrubbed by
#      archimedes itself), CURSOR_ACCESS_TOKEN (pi-cursor reads env at request
#      time), ES_DELIVERY_STATS_* (obs emit Phase 6, stashed by archimedes
#      around the worker session), GH_TOKEN (gh CLI at runtime).
unset BUILDKITE_RO_API_TOKEN
# BUILDKITE_API_TOKEN is aliased to the RO token earlier in pre-command for the
# agent's own bk_* tools; unset it too so the agent has no path to raw creds.
unset BUILDKITE_API_TOKEN
unset DEVELOCITY_API_KEY
# DEVELOCITY_ACCESS_KEY is a Gradle-Enterprise-format string derived from
# DEVELOCITY_API_ACCESS_KEY earlier in the hook. That earlier value feeds the
# Gradle build cache credential for `./gradlew` runs, not the agent tools, and
# is unrelated to the RO develocity token we just brokered — leave it alone.

export ESTC_BROKER_SOCKET
export ESTC_NONINTERACTIVE=1
export ESTC_OUTPUT_MODE=agent
export CI=true

echo "estc broker ready (pid=${ESTC_BROKER_PID}, socket=${ESTC_BROKER_SOCKET})"
