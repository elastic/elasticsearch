#!/usr/bin/env bash
# Brings up a local derived metrics playground: Elasticsearch built from this checkout, Kibana in a
# container pointed at it, a data stream configured with derived metrics, and a load generator that
# writes at a continuously varying rate.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
RUN_DIR="$HERE/.run"
# The node runs from its own copy of the distribution, so gradle never owns the running cluster.
ES_HOME="$RUN_DIR/elasticsearch"
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

# The platform-specific tar distribution, which is what `installDist` lays out under build/install.
es_distribution() {
  local os arch
  case "$(uname -s)" in
    Darwin) os=darwin ;;
    Linux)  os=linux ;;
    *) die "unsupported OS $(uname -s)" ;;
  esac
  case "$(uname -m)" in
    arm64|aarch64) arch=aarch64 ;;
    x86_64) arch="" ;;
    *) die "unsupported architecture $(uname -m)" ;;
  esac
  # the x86_64 projects are named without the architecture, e.g. darwin-tar
  [[ -n "$arch" ]] && echo "${os}-${arch}-tar" || echo "${os}-tar"
}

start_elasticsearch() {
  if es_up; then
    log "Elasticsearch is already running on ${ES}"
    return 0
  fi

  local project install
  project="$(es_distribution)"
  install="${REPO}/distribution/archives/${project}/build/install"

  if [[ ! -d "$install" ]]; then
    log "Building the ${project} distribution (first build can take a while)"
    (cd "$REPO" && ./gradlew ":distribution:archives:${project}:installDist" > "$RUN_DIR/build.log" 2>&1) \
      || die "could not build the distribution; see $RUN_DIR/build.log"
  fi

  local source
  source="$(find "$install" -maxdepth 1 -type d -name 'elasticsearch-*' | head -1)"
  [[ -n "$source" ]] || die "no distribution found under $install"

  # A fresh copy each time, so a run never inherits data or config from the last one. This is also
  # why the node is not started from the build directory itself: gradle owns that.
  log "Preparing a node from ${source##*/}"
  rm -rf "$ES_HOME"
  cp -r "$source" "$ES_HOME"

  cat > "$ES_HOME/config/elasticsearch.yml" <<YAML
cluster.name: derived-metrics-demo
node.name: derived-metrics-demo-0
discovery.type: single-node
# Beyond loopback so the Kibana container can reach the node through host.docker.internal.
network.host: ${ES_BIND_HOST}
http.port: ${ES_PORT}
xpack.security.enabled: true
# A local playground over plain HTTP; there is nothing here worth encrypting and TLS would mean
# distributing a CA to the Kibana container.
xpack.security.http.ssl.enabled: false
xpack.security.transport.ssl.enabled: false
# Machine learning is off: under a trial licence ES otherwise deploys a default ELSER endpoint,
# which downloads and indexes the model through the heap for no benefit to this demo.
xpack.ml.enabled: false
data_streams.derived_metrics.flush_interval: ${FLUSH_INTERVAL}
data_streams.derived_metrics.flush_grace_period: ${FLUSH_GRACE_PERIOD}
YAML
  printf -- "-Xms%s\n-Xmx%s\n" "${ES_HEAP}" "${ES_HEAP}" > "$ES_HOME/config/jvm.options.d/heap.options"

  "$ES_HOME/bin/elasticsearch-users" useradd "${ES_USER}" -p "${ES_PASSWORD}" -r superuser >/dev/null 2>&1 \
    || die "could not create the ${ES_USER} user"

  # Kibana connects as kibana_system rather than as the superuser above. The .kibana* indices are
  # restricted, and superuser is denied on restricted indices, so Kibana's saved-object migration
  # would terminate with a security_exception and no dashboard would ever be created.
  "$ES_HOME/bin/elasticsearch-users" useradd "${KIBANA_ES_USER}" -p "${KIBANA_ES_PASSWORD}" -r kibana_system >/dev/null 2>&1 \
    || die "could not create the ${KIBANA_ES_USER} user"

  log "Starting Elasticsearch (${ES_HEAP} heap, ${ES_LICENSE} license)"
  ES_JAVA_OPTS="" nohup "$ES_HOME/bin/elasticsearch" > "$RUN_DIR/elasticsearch.log" 2>&1 &
  echo $! > "$RUN_DIR/elasticsearch.pid"

  log "Waiting for Elasticsearch (tail -f $RUN_DIR/elasticsearch.log)"
  wait_for "elasticsearch" 300 es_up \
    || die "Elasticsearch did not start within 5m; see $RUN_DIR/elasticsearch.log"

  if [[ "${ES_LICENSE}" == "trial" ]]; then
    curl -sS -X POST "${AUTH[@]}" "${ES}/_license/start_trial?acknowledge=true" >/dev/null 2>&1 || true
  fi
  log "Elasticsearch is up on ${ES}"
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
    -e "ELASTICSEARCH_USERNAME=${KIBANA_ES_USER}" \
    -e "ELASTICSEARCH_PASSWORD=${KIBANA_ES_PASSWORD}" \
    -e "XPACK_SECURITY_ENCRYPTIONKEY=derivedmetricsdemoencryptionkey32chars" \
    -e "XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY=derivedmetricsdemosavedobjectskey32chars" \
    -e "XPACK_REPORTING_ENCRYPTIONKEY=derivedmetricsdemoreportingkey32chars000" \
    "${KIBANA_IMAGE}" >/dev/null
  log "Waiting for Kibana (docker logs -f ${KIBANA_CONTAINER})"
  wait_for "kibana" 300 kibana_up \
    || warn "Kibana is not answering yet; check 'docker logs ${KIBANA_CONTAINER}'"
}

stop_load() {
  # Match on the command line rather than the pid file: a stale generator that outlived its pid file
  # will happily keep writing, and if it beats setup.sh to a fresh cluster it auto-creates the data
  # stream with a dynamically mapped @timestamp, which then breaks every date query.
  if pgrep -f "$HERE/loadgen.py" >/dev/null 2>&1; then
    pkill -f "$HERE/loadgen.py" 2>/dev/null || true
    sleep 1
  fi
  rm -f "$RUN_DIR/loadgen.pid"
}

start_load() {
  if [[ -f "$RUN_DIR/loadgen.pid" ]] && kill -0 "$(cat "$RUN_DIR/loadgen.pid")" 2>/dev/null; then
    log "Load generator is already running (pid $(cat "$RUN_DIR/loadgen.pid"))"
    return 0
  fi
  stop_load
  log "Starting the load generator"
  nohup python3 -u "$HERE/loadgen.py" \
    --url "${ES}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}" --data-stream "${LEAN_DATA_STREAM}" --profile "${LOAD_PROFILE}" \
    > "$RUN_DIR/loadgen.log" 2>&1 &
  echo $! > "$RUN_DIR/loadgen.pid"
}

cmd_up() {
  # A generator left over from a previous run would race setup.sh for the fresh cluster.
  stop_load
  start_elasticsearch
  if [[ "${KIBANA:-}" != "skip" ]]; then
    start_kibana
  fi
  bash "$HERE/setup.sh" || warn "setup did not complete cleanly; see above"
  start_load
  cat <<BANNER

  Elasticsearch  ${ES}   (${ES_USER} / ${ES_PASSWORD})
  Kibana         http://localhost:${KIBANA_PORT}   (same credentials)
  Dashboards     http://localhost:${KIBANA_PORT}/app/dashboards#/view/derived-metrics-demo-dashboard
                 http://localhost:${KIBANA_PORT}/app/dashboards#/view/derived-metrics-demo-dashboard-lean

  rich stream         ${DATA_STREAM}
                      derived-metrics-${DATA_STREAM}-${DEFAULT_INTERVAL}   (hidden)
  lean stream         ${LEAN_DATA_STREAM}   (same documents, far fewer metrics)
                      derived-metrics-${LEAN_DATA_STREAM}-${DEFAULT_INTERVAL}   (hidden)

  ./demo.sh status    counts on both sides, plus the current derived values
  ./demo.sh logs      tail the load generator
  ./demo.sh down      stop everything

BANNER
}

cmd_status() {
  es_up || die "Elasticsearch is not running"
  python3 "$HERE/status.py" \
    --url "${ES}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}" --interval "${DEFAULT_INTERVAL}" \
    --compare-with "${LEAN_DATA_STREAM}"
  cmd_health
}

# What the feature is costing the node it runs on, as opposed to what it is producing. All three are
# things the feature bounds deliberately, so seeing them at rest is as informative as seeing them
# under strain.
cmd_health() {
  es_up || die "Elasticsearch is not running"
  echo
  log "Node cost"
  curl -sS "${AUTH[@]}" "${ES}/_nodes/stats/breaker,thread_pool?filter_path=nodes.*.breakers.derived_metrics,nodes.*.thread_pool.derived_metrics" \
    | python3 "$HERE/health.py"
}

# The same question asked off the derived metrics and off the raw stream, timed cold and warm. This is
# the query-side half of the argument; ./demo.sh status is the storage-side half.
cmd_bench() {
  es_up || die "Elasticsearch is not running"
  python3 "$HERE/bench.py" \
    --url "${ES}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
    --data-stream "${DATA_STREAM}" --interval "${DEFAULT_INTERVAL}"
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
  if [[ -f "$RUN_DIR/elasticsearch.pid" ]]; then
    local pid
    pid="$(cat "$RUN_DIR/elasticsearch.pid")"
    if kill -0 "$pid" 2>/dev/null; then
      log "Stopping Elasticsearch (pid ${pid})"
      kill "$pid" 2>/dev/null || true
    fi
    rm -f "$RUN_DIR/elasticsearch.pid"
  fi
  # Belt and braces: match by this demo's own node home, which cannot collide with another cluster.
  local pids
  pids=$(pgrep -f "es.path.home=${ES_HOME}" || true)
  if [[ -n "$pids" ]]; then
    # shellcheck disable=SC2086
    kill $pids 2>/dev/null || true
  fi
  local deadline=$((SECONDS + 60))
  while ((SECONDS < deadline)); do
    es_up || return 0
    sleep 2
  done
  warn "the Elasticsearch node is still running; kill it manually if needed"
}

cmd_down() {
  log "Stopping the load generator"
  stop_load
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
  health)  cmd_health ;;
  bench)   cmd_bench ;;
  logs)    cmd_logs ;;
  window)  cmd_window "${2:-5}" ;;
  eslogs)  cmd_eslogs ;;
  setup)   bash "$HERE/setup.sh" ;;
  bootstrap-kibana) bash "$HERE/bootstrap-kibana.sh" ;;
  load)    start_load ;;
  *)       die "usage: $0 [up|down|status|health|bench|window|logs|eslogs|setup|bootstrap-kibana|load]" ;;
esac
