#!/usr/bin/env bash
# Creates the demo's Kibana data views and dashboards. Safe to run at any time: it retitles data views
# whose target has changed and overwrites the dashboards in place.
#
# Run it directly with ./demo.sh bootstrap-kibana whenever Kibana was not ready during ./demo.sh up,
# or after Kibana or Elasticsearch has been restarted.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=config.env
source "$HERE/config.env"

KB="http://localhost:${KIBANA_PORT}"
AUTH=(-u "${ES_USER}:${ES_PASSWORD}")
# How long to wait for Kibana to become usable. Deliberately bounded: if it is not ready by now
# something is wrong and saying so beats hanging.
WAIT_SECONDS="${KIBANA_WAIT_SECONDS:-90}"

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m!!\033[0m %s\n' "$*"; }
die()  { printf '\033[1;31mxx\033[0m %s\n' "$*" >&2; exit 1; }

# Probe the API this script actually needs rather than /api/status. Kibana answers 200 on /api/status
# while still unable to serve saved objects, which is exactly the window that made ./demo.sh up skip
# this step and report Kibana as unreachable.
saved_objects_code() {
  curl -s -m 15 -o /dev/null -w '%{http_code}' "${AUTH[@]}" \
    "${KB}/api/saved_objects/_find?type=index-pattern&per_page=1" 2>/dev/null || echo 000
}

diagnose() {
  local code=$1
  case "$code" in
    000) warn "Kibana is not answering on ${KB} at all."
         warn "  is the container running?   docker ps --filter name=${KIBANA_CONTAINER}" ;;
    401|403) warn "Kibana is up but rejected ${ES_USER}."
             warn "  usually means Kibana is still bound to a previous Elasticsearch; restart it:"
             warn "  docker restart ${KIBANA_CONTAINER}" ;;
    503) warn "Kibana is up but not serving yet, most often because it cannot reach Elasticsearch."
         warn "  check:  docker logs --tail 20 ${KIBANA_CONTAINER}"
         warn "  the container reaches the host as ${ES_HOST_FROM_CONTAINER}:${ES_PORT}" ;;
    *)   warn "Kibana returned ${code} for the saved objects API." ;;
  esac
}

log "Waiting up to ${WAIT_SECONDS}s for Kibana to serve saved objects"
deadline=$((SECONDS + WAIT_SECONDS))
code=$(saved_objects_code)
while [[ "$code" != "200" ]] && ((SECONDS < deadline)); do
  sleep 5
  code=$(saved_objects_code)
done

if [[ "$code" != "200" ]]; then
  diagnose "$code"
  die "Kibana is not usable; nothing was created. Fix the above and re-run ./demo.sh bootstrap-kibana"
fi
log "Kibana is serving saved objects"

log "Ensuring data views"
python3 "$HERE/dataviews.py" \
  --kibana "${KB}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
  --data-stream "${DATA_STREAM}" --interval "${DEFAULT_INTERVAL}"
python3 "$HERE/dataviews.py" \
  --kibana "${KB}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
  --data-stream "${LEAN_DATA_STREAM}" --interval "${DEFAULT_INTERVAL}" --label lean

log "Creating dashboards"
python3 "$HERE/dashboard.py" \
  --kibana "${KB}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
  --data-stream "${DATA_STREAM}" --interval "${DEFAULT_INTERVAL}"
python3 "$HERE/dashboard.py" \
  --kibana "${KB}" --user "${ES_USER}" --password "${ES_PASSWORD}" \
  --data-stream "${LEAN_DATA_STREAM}" --interval "${DEFAULT_INTERVAL}" \
  --lean --compare-with "${DATA_STREAM}"

echo
echo "  Kibana bootstrap complete."
