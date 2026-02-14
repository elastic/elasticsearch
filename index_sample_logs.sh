#!/usr/bin/env bash
set -euo pipefail

ES_URL="${ES_URL:-http://localhost:9200}"
INDEX="sample-logs"
TOTAL=1000000
BATCH=5000

# Sample log messages to pick from
MESSAGES=(
  "User logged in successfully"
  "Failed login attempt from unknown IP"
  "Request processed in 42ms"
  "Connection timeout after 30s"
  "Database query returned 0 results"
  "Cache miss for session token"
  "File upload completed: report.pdf"
  "Disk usage warning: 89% utilized"
  "Scheduled backup started"
  "API rate limit exceeded"
  "TLS handshake failed with peer"
  "Out of memory: killing process 4521"
  "New deployment rolled out: v2.13.7"
  "Health check passed on port 8080"
  "Garbage collection paused for 120ms"
  "DNS resolution failed for upstream host"
  "Message queue depth exceeds threshold"
  "Configuration reloaded from disk"
  "Certificate expires in 14 days"
  "Shard rebalancing initiated"
)
NUM_MESSAGES=${#MESSAGES[@]}

# Base timestamp: 30 days ago, in seconds
BASE_TS=$(date -d "30 days ago" +%s 2>/dev/null || date -v-30d +%s)
# Span: 30 days in seconds
SPAN=$((30 * 24 * 3600))

echo "Creating index '$INDEX' with explicit mapping..."
curl -uelastic:password -s -X DELETE "$ES_URL/$INDEX" > /dev/null 2>&1 || true
curl -uelastic:password -s -X PUT "$ES_URL/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": { "number_of_shards": 1, "number_of_replicas": 0, "refresh_interval": "-1" },
  "mappings": {
    "properties": {
      "@timestamp": { "type": "date" },
      "message":    { "type": "keyword" }
    }
  }
}' | cat
echo ""

echo "Indexing $TOTAL documents in batches of $BATCH..."

sent=0
while (( sent < TOTAL )); do
  batch_size=$(( TOTAL - sent < BATCH ? TOTAL - sent : BATCH ))
  body=""
  rm -rf /tmp/bulk
  for (( i = 0; i < batch_size; i++ )); do
    offset=$(( RANDOM * RANDOM % SPAN ))
    ts=$(( BASE_TS + offset ))
    # Format as ISO-8601 with milliseconds
    ts_fmt=$(date -u -d "@$ts" '+%Y-%m-%dT%H:%M:%S' 2>/dev/null || date -u -r "$ts" '+%Y-%m-%dT%H:%M:%S')
    ms=$(printf "%03d" $(( RANDOM % 1000 )))
    msg="${MESSAGES[$(( RANDOM % NUM_MESSAGES ))]}"
    echo '{"index":{}}' >> /tmp/bulk
    echo "{\"@timestamp\":\"${ts_fmt}.${ms}Z\",\"message\":\"${msg}\"}" >> /tmp/bulk
  done
  response=$(curl -uelastic:password -s -X POST "$ES_URL/$INDEX/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/bulk)
  errors=$(echo "$response" | grep -o '"errors":\(true\|false\)' | head -1)
  sent=$(( sent + batch_size ))
  pct=$(( sent * 100 / TOTAL ))
  printf "\r  %d / %d  (%d%%)  [%s]" "$sent" "$TOTAL" "$pct" "$errors"
done

echo ""
echo "Refreshing index..."
curl -uelastic:password -s -X POST "$ES_URL/$INDEX/_refresh" | cat
echo ""

echo "Verifying document count..."
curl -uelastic:password -s "$ES_URL/$INDEX/_count" | cat
echo ""
echo "Done."
