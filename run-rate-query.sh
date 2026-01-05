#!/usr/bin/env bash
# Sends an ES|QL query to localhost:9200 and prints row count and duration (ms)

ES_URL="http://localhost:9200/_query"
QUERY='TS metrics-hostmetricsreceiver.otel-default | STATS avg = AVG(RATE(`metrics.system.cpu.time`)) BY host.name, TBUCKET(5m)'

for i in {1..30}; do
  start_ms=$(gdate +%s%3N)
  response=$(curl -s -H "Content-Type: application/json" -X POST "$ES_URL" -d "{\"query\":\"$QUERY\"}")
  end_ms=$(gdate +%s%3N)
  duration_ms=$((end_ms - start_ms))

  # ES|QL tabular responses contain a top-level "values" array
  rows=$(jq -r '.values | length' <<< "$response")

  echo "run=$i rows=$rows"
  echo "run=$i duration_ms=$duration_ms"
done

