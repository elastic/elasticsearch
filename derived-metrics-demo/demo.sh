#!/usr/bin/env bash
# Brings up a local derived metrics playground: Elasticsearch built from this checkout, Kibana in a
# container pointed at it, a data stream configured with derived metrics, and a load generator that
# writes at a continuously varying rate.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
RUN_DIR="$HERE/.run"
# shellcheck source=config.env
source "$HERE/config.env"

ES="http://localhost:${ES_PORT}"
AUTH=(-u "${ES_USER}:${ES_PASSWORD}")

mkdir -p "$RUN_DIR"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

es_up()     { curl -s -m 3 -o /dev/null "${AUTH[@]}" "${ES}/_cluster/health" 2>/dev/null; }
kibana_up() { curl -s -m 3 -o /dev/null "http://localhost:${KIBANA_PORT}/api/status" 2>/dev/null; }

wait_for() {
  local what=$1 seconds=$2
  shift 2
  local deadline=$((SECONDS + seconds))
  while ((SECONDS < deadline)); do
    if "$@"; then return 0; fi
    sleep 2
  done
  return 1
}

require_docker() {
  command -v docker >/dev/null || die "docker is not installed; install Docker Desktop or set KIBANA=skip"
  if docker info >/dev/null 2>&1; then return 0; fi
  if [[ "$(uname -s)" == "Darwin" ]]; then
    log "Docker daemon is not running, starting Docker Desktop"
    open -a Docker || die "could not start Docker Desktop; start it manually and re-run"
    wait_for "docker" 120 docker info >/dev/null 2>&1 \
      || die "Docker daemon did not come up within 120s"
  else
    die "Docker daemon is not running; start it and re-run"
  fi
}

start_elasticsearch() {
  if es_up; then
    log "Elasticsearch is already running on ${ES}"
    return 0
  fi
  log "Building and starting Elasticsearch from ${REPO} (first build can take a while)"
  # http.host beyond loopback so the Kibana container can reach us. The transport layer stays on
  # loopback, which keeps the node out of production bootstrap checks.
  (
    cd "$REPO"
    nohup ./gradlew run \
      -Drun.license_type="${ES_LICENSE}" \
      -Dtests.es.http.host="${ES_BIND_HOST}" \
      -Dtests.es.data_streams.derived_metrics.flush_interval="${FLUSH_INTERVAL}" \
      -Dtests.es.data_streams.derived_metrics.flush_grace_period="${FLUSH_GRACE_PERIOD}" \
      > "$RUN_DIR/elasticsearch.log" 2>&1 &
    echo $! > "$RUN_DIR/elasticsearch.pid"
  )
  log "Waiting for Elasticsearch (tail -f $RUN_DIR/elasticsearch.log)"
  wait_for "elasticsearch" 900 es_up \
    || die "Elasticsearch did not start within 15m; see $RUN_DIR/elasticsearch.log"
  log "Elasticsearch is up on ${ES} (${ES_LICENSE} license)"
}

start_kibana() {
  require_docker
  if [[ -n "$(docker ps -q -f "name=^${KIBANA_CONTAINER}$")" ]]; then
    log "Kibana container is already running"
    return 0
  fi
  docker rm -f "${KIBANA_CONTAINER}" >/dev/null 2>&1 || true
  log "Starting Kibana (${KIBANA_IMAGE})"
  docker run -d --name "${KIBANA_CONTAINER}" \
    -p "${KIBANA_PORT}:5601" \
    --memory "${KIBANA_MEMORY}" \
    -e "NODE_OPTIONS=--max-old-space-size=${KIBANA_HEAP_MB}" \
    -e "ELASTICSEARCH_HOSTS=http://${ES_HOST_FROM_CONTAINER}:${ES_PORT}" \
    -e "ELASTICSEARCH_USERNAME=${ES_USER}" \
    -e "ELASTICSEARCH_PASSWORD=${ES_PASSWORD}" \
    -e "XPACK_SECURITY_ENCRYPTIONKEY=derivedmetricsdemoencryptionkey32chars" \
    -e "XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY=derivedmetricsdemosavedobjectskey32chars" \
    -e "XPACK_REPORTING_ENCRYPTIONKEY=derivedmetricsdemoreportingkey32chars000" \
    "${KIBANA_IMAGE}" >/dev/null
  log "Waiting for Kibana (docker logs -f ${KIBANA_CONTAINER})"
  wait_for "kibana" 300 kibana_up \
    || warn "Kibana is not answering yet; check 'docker logs ${KIBANA_CONTAINER}'"
}

start_load() {
  if [[ -f "$RUN_DIR/loadgen.pid" ]] && kill -0 "$(cat "$RUN_DIR/loadgen.pid")" 2>/dev/null; then
    log "Load generator is already running (pid $(cat "$RUN_DIR/loadgen.pid"))"
    return 0
  fi
  log "Starting the load generator"
  nohup python3 -u "$HERE/loadgen.py" \
    --url "${ES}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}" --profile "${LOAD_PROFILE}" \
    > "$RUN_DIR/loadgen.log" 2>&1 &
  echo $! > "$RUN_DIR/loadgen.pid"
}

cmd_up() {
  start_elasticsearch
  if [[ "${KIBANA:-}" != "skip" ]]; then
    start_kibana
  fi
  bash "$HERE/setup.sh"
  start_load
  cat <<BANNER

  Elasticsearch  ${ES}   (${ES_USER} / ${ES_PASSWORD})
  Kibana         http://localhost:${KIBANA_PORT}   (same credentials)
  Dashboard      http://localhost:${KIBANA_PORT}/app/dashboards#/view/derived-metrics-demo-dashboard

  source stream       ${DATA_STREAM}
  derived metrics     derived-metrics-${DATA_STREAM}   (hidden data stream)

  ./demo.sh status    counts on both sides, plus the current derived values
  ./demo.sh logs      tail the load generator
  ./demo.sh down      stop everything

BANNER
}

cmd_status() {
  es_up || die "Elasticsearch is not running"
  python3 "$HERE/status.py" \
    --url "${ES}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}"
}

# A closed, 10s-aligned window, for the side-by-side queries in compare.console. Closed because the
# most recent interval may not have been flushed yet; aligned because derived documents are stamped
# at the start of their interval.
cmd_window() {
  python3 - "${1:-5}" <<'PYTHON'
import datetime, sys
minutes = int(sys.argv[1])
now = datetime.datetime.now(datetime.timezone.utc)
end = now - datetime.timedelta(seconds=30)
end = end.replace(second=end.second // 10 * 10, microsecond=0)
start = end - datetime.timedelta(minutes=minutes)
fmt = "%Y-%m-%dT%H:%M:%S.000Z"
print(f'  from  "{start.strftime(fmt)}"')
print(f'  to    "{end.strftime(fmt)}"')
print()
print("Paste into compare.console, replacing WINDOW_START and WINDOW_END.")
PYTHON
}

cmd_logs() { tail -f "$RUN_DIR/loadgen.log"; }
cmd_eslogs() { tail -f "$RUN_DIR/elasticsearch.log"; }

stop_elasticsearch() {
  # ./gradlew run launches the node from the gradle daemon, so the pid we recorded for the gradle
  # client is not the node's parent and killing its children does not reach it. Match the node by
  # the distribution path inside this checkout, which cannot collide with another cluster.
  local pids
  pids=$(pgrep -f "es.path.home=${REPO}/distribution" || true)
  if [[ -n "$pids" ]]; then
    log "Stopping the Elasticsearch node (${pids//$'\n'/ })"
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
  fi
  if [[ -f "$RUN_DIR/elasticsearch.pid" ]]; then
    kill "$(cat "$RUN_DIR/elasticsearch.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/elasticsearch.pid"
  fi
  local deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    pgrep -f "es.path.home=${REPO}/distribution" >/dev/null || return 0
    sleep 2
  done
  warn "the Elasticsearch node is still running; kill it manually if needed"
}

cmd_down() {
  if [[ -f "$RUN_DIR/loadgen.pid" ]]; then
    log "Stopping the load generator"
    kill "$(cat "$RUN_DIR/loadgen.pid")" 2>/dev/null || true
    rm -f "$RUN_DIR/loadgen.pid"
  fi
  if command -v docker >/dev/null && docker info >/dev/null 2>&1; then
    log "Removing the Kibana container"
    docker rm -f "${KIBANA_CONTAINER}" >/dev/null 2>&1 || true
  fi
  stop_elasticsearch
  log "Done"
}

case "${1:-up}" in
  up)      cmd_up ;;
  down)    cmd_down ;;
  status)  cmd_status ;;
  logs)    cmd_logs ;;
  window)  cmd_window "${2:-5}" ;;
  eslogs)  cmd_eslogs ;;
  setup)   bash "$HERE/setup.sh" ;;
  load)    start_load ;;
  *)       die "usage: $0 [up|down|status|window|logs|eslogs|setup|load]" ;;
esac
