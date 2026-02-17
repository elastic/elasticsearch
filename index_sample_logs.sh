#!/usr/bin/env bash
set -euo pipefail

# NOCOMMIT this whole thing
# cursor did most of this. It's weird looking but who cares.

ES_URL="${ES_URL:-http://localhost:9200}"
INDEX="sample-logs-sorted"
TOTAL=100000000
BATCH=10000

# Sample log messages, pipe-delimited for awk
MSG_STRING="User logged in successfully|Failed login attempt from unknown IP|Request processed in 42ms|Connection timeout after 30s|Database query returned 0 results|Cache miss for session token|File upload completed: report.pdf|Disk usage warning: 89% utilized|Scheduled backup started|API rate limit exceeded|TLS handshake failed with peer|Out of memory: killing process 4521|New deployment rolled out: v2.13.7|Health check passed on port 8080|Garbage collection paused for 120ms|DNS resolution failed for upstream host|Message queue depth exceeds threshold|Configuration reloaded from disk|Certificate expires in 14 days|Shard rebalancing initiated|Unexpected null pointer in request handler|Worker thread pool exhausted|Index merge completed in 3.2s|Snapshot repository verification succeeded|Slow query detected: 1200ms execution time|Inbound connection rejected: max clients reached|Node joined cluster: node-07|Replica promotion completed for shard 3|Circuit breaker tripped: parent limit reached|Fielddata eviction: freed 512MB|Authentication token refreshed for service account|Watcher alert triggered: high CPU usage|Audit log rotation completed|Pipeline ingest failure: parsing error on field geo.location|Cluster state update applied in 85ms|License expires in 30 days|Cross-cluster search completed in 340ms|Transform checkpoint created: id 42|Anomaly detected: unusual login pattern|Rollover condition met: max_age 7d|Data stream backing index created|ILM policy transition: hot to warm|Plugin compatibility check passed|Keystore reloaded with 3 secure settings|SAML assertion validated for realm sso1|Task cancelled by user: reindex from remote|Routing allocation delayed: node left cluster|Dangling index imported: old-metrics-2024|Searchable snapshot mounted: logs-archive|Vector similarity search returned 25 results|Segment flush triggered on transaction log size|Read request routed to replica shard 2|Write-ahead log compacted: reclaimed 1.4GB|Coordinating node selected for aggregation|Bulk indexing throughput: 18k docs/s|Mapping update applied: added field user.roles|Index template matched pattern: metrics-*|Translog sync completed in 12ms|Peer recovery started for shard 5 from node-03|Primary term incremented to 7|Sequence number checkpoint advanced to 482910|Retention lease renewed for follower index|Frozen tier search latency: 890ms|Deprecation warning: _type field used in query|Enrich policy execution completed in 4.2s|GeoIP database updated: GeoLite2-City|Ingest pipeline processor failed: grok pattern mismatch|Runtime field evaluated: 350us avg per doc|Fetch phase collected 500 hits from 5 shards|Scroll context cleared after idle timeout|Point-in-time reader opened: keep_alive 5m|Composite aggregation paged through 12 buckets|Significant terms found in anomaly corpus|Percentiles sketch merged across 3 shards|Nested document count: 2.1M in parent index|Flattened field indexed: labels with 47 keys|Painless script compiled and cached|Stored script execution took 28ms|Force merge reduced segment count from 24 to 1|Shrink index operation started: 8 shards to 1|Split index completed: 1 shard to 4|Clone index created: logs-backup-2025|Alias swap completed: logs-current points to logs-000042|Reindex task progress: 67% at 22k docs/s|Update by query matched 14302 documents|Delete by query removed 891 stale records|Field caps response: 312 fields across 8 indices|Cluster allocation explain: shard unassigned due to node filters|Hot threads captured: 3 threads in search phase|Pending tasks queue depth: 7 cluster state updates|Adaptive replica selection routed to node-02|Breaker estimation: request circuit at 78% capacity|Node stats collected: heap usage 64% of 31GB|Index stats refreshed: 42GB total store size|Cat indices response served in 15ms|Remote cluster connection established: cluster-west|CCR follower index lagging by 230 operations|Voting configuration updated: added node-09|Master stability check: 3 consecutive successful pings"

# Base timestamp: 30 days ago, in seconds
BASE_TS=$(date -d "30 days ago" +%s 2>/dev/null || date -v-30d +%s)
# Span: 30 days in seconds
SPAN=$((30 * 24 * 3600))

echo "Creating index '$INDEX' with explicit mapping..."
curl -uelastic:password -s -X DELETE "$ES_URL/$INDEX" > /dev/null 2>&1 || true
curl -uelastic:password -s -X PUT "$ES_URL/$INDEX" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "-1",
    "index": {
      "sort.field" : "@timestamp",
      "sort.order" : "asc"
    }
  },
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

  # Generate bulk body entirely in awk — no subprocess per document
  awk -v base_ts="$BASE_TS" -v span="$SPAN" -v n="$batch_size" \
      -v msgs="$MSG_STRING" -v seed="$RANDOM" '
  BEGIN {
    srand(seed)
    num_msg = split(msgs, m, "|")
    for (i = 1; i <= n; i++) {
      offset = int(rand() * span)
      ts = base_ts + offset
      ms = int(rand() * 1000)
      ts_fmt = strftime("%Y-%m-%dT%H:%M:%S", ts, 1)
      msg = m[int(rand() * num_msg) + 1]
      print "{\"index\":{}}"
      printf "{\"@timestamp\":\"%s.%03dZ\",\"message\":\"%s\"}\n", ts_fmt, ms, msg
    }
  }' /dev/null > /tmp/bulk

  response=$(curl -uelastic:password -s -X POST "$ES_URL/$INDEX/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @/tmp/bulk)
  errors=$(echo "$response" | grep -o '"errors":\(true\|false\)' | head -1)
  sent=$(( sent + batch_size ))
  pct=$(( sent * 100 / TOTAL ))
  printf "\r  %d / %d  (%d%%)  [%s]" "$sent" "$TOTAL" "$pct" "$errors"
  curl -uelastic:password -s -X POST "$ES_URL/$INDEX/_flush" >> /dev/null
done

echo ""
echo "Refreshing index..."
curl -uelastic:password -s -X POST "$ES_URL/$INDEX/_refresh" | cat
echo ""

echo "Verifying document count..."
curl -uelastic:password -s "$ES_URL/$INDEX/_count" | cat
echo ""
echo "Done."
