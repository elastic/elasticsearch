---
description: Ingest sample Prometheus metrics into Elasticsearch and query them with PromQL through the Prometheus-compatible HTTP API and ES|QL.
navigation_title: Get started
applies_to:
  stack: preview 9.4, ga 9.5
  serverless: ga
type: tutorial
products:
  - id: elasticsearch
---

# Get started with PromQL in {{es}} [promql-tutorial]

Learn how to ingest sample Prometheus metrics into {{es}} and query them with [PromQL](/reference/query-languages/promql.md). This tutorial walks you through ingesting a sample counter metric, running PromQL queries with the [Prometheus-compatible HTTP API](promql-http-api.md), post-processing PromQL results with {{esql}}, and connecting Prometheus with remote write.

This tutorial assumes you have basic knowledge of:

- Prometheus metrics, samples, and labels
- REST APIs and JSON
- Basic PromQL selectors and functions

By the end of this tutorial, you can:

- Ingest Prometheus-style samples into a [time series index](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
- Run instant PromQL queries with label matchers, aggregations, and `rate()`
- Run PromQL as an {{esql}} source command and post-process the results with {{esql}}
- Configure Prometheus remote write to ingest metrics continuously

## Before you begin

To get started with PromQL in {{es}}, you need the following:

- An {{es}} deployment with PromQL support
- Permissions to create an index and write and read documents in it
- Access to [Console](docs-content://explore-analyze/query-filter/tools/console.md) in {{kib}}, or a terminal with `curl`

::::{note}
If you use `curl`, set the following environment variables to simplify the commands in this tutorial:

```bash
export ELASTICSEARCH_URL="your-elasticsearch-url"
export API_KEY="your-api-key"
```
::::

This tutorial uses fixed sample timestamps on 2026-10-22 so that every query returns the same result whenever you run it. To accept those timestamps, it stores the samples in a dedicated time series index with explicit time bounds instead of the default Prometheus data stream. The index mapping is a reduced version of the built-in `metrics-prometheus@mappings` component template that remote write uses: it only maps `_total` metrics as counters and omits the gauge and other counter mappings, so don't reuse it for production data. Metrics that you ingest with [Prometheus remote write](#promql-tutorial-remote-write) use current timestamps and don't need this setup.

You can also get help from your coding agent to write PromQL queries:

:::{agent-skill}
:url: https://github.com/elastic/agent-skills@elasticsearch-esql
Agent skill that assists with ES|QL query generation including the PROMQL command.
:::

## Step 0: Add sample Prometheus metrics

This tutorial uses an example counter metric, `requests_total`, that tracks HTTP requests for two instances, `us-east-1` and `us-west-1`. Each instance has three samples, one minute apart. The `us-east-1` counter increases by 7 requests per minute, and the `us-west-1` counter increases by 3 requests per minute.

Create the `promql-tutorial` time series index:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/create-index.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X PUT "${ELASTICSEARCH_URL}/promql-tutorial" \
     -H "Authorization: ApiKey ${API_KEY}" \
     -H "Content-Type: application/json" \
     -d '{
       "settings": {
         "index": {
           "mode": "time_series",
           "routing_path": ["labels.*"],
           "time_series": {
             "start_time": "2026-10-22T11:00:00Z",
             "end_time": "2026-10-22T13:00:00Z"
           }
         }
       },
       "mappings": {
         "dynamic_templates": [
           {
             "counter": {
               "path_match": ["metrics.*_total"],
               "mapping": { "type": "double", "time_series_metric": "counter" }
             }
           }
         ],
         "properties": {
           "@timestamp": { "type": "date" },
           "labels": { "type": "passthrough", "priority": 10, "time_series_dimension": true },
           "metrics": { "type": "passthrough", "priority": 20 }
         }
       }
     }'
```
:::

::::

Add the sample metrics with the bulk API:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/bulk-ingest-samples.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/promql-tutorial/_bulk?refresh" \
     -H "Authorization: ApiKey ${API_KEY}" \
     -H "Content-Type: application/x-ndjson" \
     --data-binary @- << 'EOF'
{ "create": {} }
{"@timestamp":"2026-10-22T12:05:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":170}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:05:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":80}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:06:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":177}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:04:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-east-1"},"metrics":{"requests_total":163}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:04:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":77}}
{ "create": {} }
{"@timestamp":"2026-10-22T12:06:49Z","labels":{"__name__":"requests_total","job":"tutorial","instance":"us-west-1"},"metrics":{"requests_total":83}}
EOF
```
:::

::::

**Result:** The response confirms that all six documents were created, with `"errors": false` and a `created` result for each item.

::::{dropdown} Example response (truncated)
```json
{
  "errors": false,
  "took": 12,
  "items": [
    {
      "create": {
        "_index": "promql-tutorial",
        "_id": "LDL-KSuugm8OtHL48HlRdj02ELXbaZuh8sbT1HvAVjf7WZWwiRB_MV...",
        "_version": 1,
        "result": "created",
        "status": 201
      }
    }
  ]
}
```
::::

In practice, you ingest Prometheus metrics continuously with remote write or another collection pipeline instead of the bulk API. Refer to [Step 3: Connect Prometheus with remote write](#promql-tutorial-remote-write).

## Step 1: Run basic PromQL queries with the HTTP API

{{es}} exposes a [Prometheus-compatible HTTP API](promql-http-api.md) for running PromQL against stored metrics. The queries in this step use the [instant query](promql-http-api.md#promql-http-api-query-instant) endpoint, scoped to the `promql-tutorial` index with the `/_prometheus/{index}/api/v1/query` path form. Each query sets `time=1792670839`, which is 2026-10-22T12:07:19Z, thirty seconds after the last sample. If you omit `time`, the query runs at the current time.

### Run an instant query

Retrieve the latest value of `requests_total` for each instance:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/query-instant.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -G "${ELASTICSEARCH_URL}/_prometheus/promql-tutorial/api/v1/query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     --data-urlencode "query=requests_total" \
     --data-urlencode "time=1792670839"
```
:::

::::

**Result:** The response has a `resultType` of `vector` with two series, one per instance. Each series includes its labels and its latest counter value: `177.0` for `us-east-1` and `83.0` for `us-west-1`.

::::{dropdown} Example response
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {
          "__name__": "requests_total",
          "instance": "us-east-1",
          "job": "tutorial"
        },
        "value": [1.792670839E9, "177.0"]
      },
      {
        "metric": {
          "__name__": "requests_total",
          "instance": "us-west-1",
          "job": "tutorial"
        },
        "value": [1.792670839E9, "83.0"]
      }
    ]
  }
}
```
::::

### Filter with label matchers

Use label matchers to select specific series. The following query only returns the `us-east-1` series:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/query-label-matcher.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -G "${ELASTICSEARCH_URL}/_prometheus/promql-tutorial/api/v1/query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     --data-urlencode 'query=requests_total{instance="us-east-1"}' \
     --data-urlencode "time=1792670839"
```
:::

::::

**Result:** The response contains one series, with `instance="us-east-1"` and a value of `177.0`.

### Aggregate across series

Aggregate values across instances with `sum()`. The `by(job)` clause groups the result by the `job` label:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/query-sum-by-job.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -G "${ELASTICSEARCH_URL}/_prometheus/promql-tutorial/api/v1/query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     --data-urlencode "query=sum by(job)(requests_total)" \
     --data-urlencode "time=1792670839"
```
:::

::::

**Result:** The response contains one series, with `job="tutorial"` and a value of `260.0`, the sum of both instance counters.

### Select the largest series with `topk()` [promql-tutorial-topk]

{applies_to}`stack: ga 9.5` {applies_to}`serverless: ga`

Use `topk()` to select the series with the largest values. The following query returns the instance with the highest request count:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/query-topk.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -G "${ELASTICSEARCH_URL}/_prometheus/promql-tutorial/api/v1/query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     --data-urlencode "query=topk(1,requests_total)" \
     --data-urlencode "time=1792670839"
```
:::

::::

**Result:** The response contains one series, with `instance="us-east-1"` and a value of `177.0`.

### Calculate a per-second rate with `rate()`

Use `rate()` to calculate the per-second increase of a counter over a time window. The following query sums the request rate over the last five minutes across both instances:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/query-rate.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -G "${ELASTICSEARCH_URL}/_prometheus/promql-tutorial/api/v1/query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     --data-urlencode "query=sum(rate(requests_total[5m]))" \
     --data-urlencode "time=1792670839"
```
:::

::::

**Result:** The response contains one series with an empty label set and a value of `0.1`, the combined request rate in requests per second.

::::{dropdown} Example response
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {},
        "value": [1.792670839E9, "0.1"]
      }
    ]
  }
}
```
::::

## Step 2: Process PromQL results with {{esql}} [promql-tutorial-esql]

You can also run PromQL as an {{esql}} source command. Unlike the HTTP API, which returns Prometheus-compatible JSON, the [`PROMQL` command](/reference/query-languages/esql/commands/promql.md) returns tabular results that you can process with standard {{esql}} commands.

The following query calculates the combined request rate every minute for a twelve-minute range that ends thirty seconds after the last sample. The `start` and `end` options set the range, `step` sets the resolution, and `http_rate` names the result column:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/esql-promql-range.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query?format=txt" \
     -H "Authorization: ApiKey ${API_KEY}" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "PROMQL index=promql-tutorial start=1792670119 end=1792670839 step=60s http_rate=(sum(rate(requests_total[5m]))) | SORT step DESC"
     }'
```
:::

::::

**Result:** The response is a table with `http_rate` and `step` columns. It contains one row for each minute in which a rate can be calculated from the samples.

::::{dropdown} Example response
```text
     http_rate     |          step
-------------------+------------------------
0.1                |2026-10-22T12:07:19.000Z
0.06666666666666667|2026-10-22T12:06:19.000Z
```
::::

Extend the same query with {{esql}} to filter out low rates and calculate the average and peak rate:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/esql-promql-stats.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X POST "${ELASTICSEARCH_URL}/_query" \
     -H "Authorization: ApiKey ${API_KEY}" \
     -H "Content-Type: application/json" \
     -d '{
       "query": "PROMQL index=promql-tutorial start=1792670119 end=1792670839 step=60s http_rate=(sum(rate(requests_total[5m]))) | WHERE http_rate > 0.07 | STATS avg_rate = AVG(http_rate), peak_rate = MAX(http_rate)"
     }'
```
:::

::::

**Result:** The response contains one row with `avg_rate` and `peak_rate` columns. For the sample data, the filter drops the `0.0667` row at 12:06:19, so both the average and the peak rate are `0.1` requests per second.

::::{dropdown} Example response (truncated)
```json
{
  "columns": [
    { "name": "avg_rate", "type": "double" },
    { "name": "peak_rate", "type": "double" }
  ],
  "values": [
    [0.1, 0.1]
  ]
}
```
::::

::::{tip}
In {{kib}}, you can omit `start` and `end` so that the query follows the date picker, and omit range selectors such as `[5m]` so that the window adapts to the time range and step. Refer to [`PROMQL` command](/reference/query-languages/esql/commands/promql.md) for the full syntax and best practices.
::::

## Step 3: Connect Prometheus with remote write [promql-tutorial-remote-write]

To ingest metrics continuously, configure Prometheus to send samples directly to {{es}} with remote write. Add the following configuration to your `prometheus.yml` file, replacing `<es-endpoint>` and `<api-key>` with your endpoint and credentials. The API key requires write access to the target data stream.

```yaml
remote_write:
  - url: "https://<es-endpoint>/_prometheus/api/v1/write"
    authorization:
      type: ApiKey
      credentials: "<api-key>"
```

By default, metrics are stored in the `metrics-generic.prometheus-default` data stream. To write to a custom dataset, include it in the endpoint URL:

```yaml
remote_write:
  - url: "https://<es-endpoint>/_prometheus/metrics/tutorial/api/v1/write"
    authorization:
      type: ApiKey
      credentials: "<api-key>"
```

This configuration writes to `metrics-tutorial.prometheus-default`. The query shapes from the previous steps work against remote-write metrics with your own metric names in place of `requests_total`, and without an index segment in the URL, because the HTTP API queries `metrics-*` by default. For example, use `/_prometheus/api/v1/query` instead of `/_prometheus/promql-tutorial/api/v1/query`, and omit `time` to query at the current time. For more information, refer to [Ingest data with Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md).

## Clean up resources (optional) [promql-tutorial-clean-up]

Delete the tutorial index and its sample data:

::::{tab-set}
:group: api-examples

:::{tab-item} Console
:sync: console
:::{include} _snippets/promql-tutorial/delete-index.md
:::
:::

:::{tab-item} curl
:sync: curl
```bash
curl -X DELETE "${ELASTICSEARCH_URL}/promql-tutorial" \
     -H "Authorization: ApiKey ${API_KEY}"
```
:::

::::

## Summary

In this tutorial, you learned how to:

- Ingest Prometheus-style samples into a time series index
- Run instant PromQL queries, label matcher filters, aggregations, and rate calculations with the Prometheus-compatible HTTP API
- Run PromQL with the {{esql}} `PROMQL` command and post-process the results with `WHERE` and `STATS`
- Configure Prometheus remote write for continuous ingestion

## Next steps

- Connect Grafana and run PromQL from dashboards. Refer to [Prometheus data source in Grafana](promql-grafana.md).
- Explore the supported PromQL [functions](functions.md) and [operators](operators.md), including differences from Prometheus.
- Review the behavioral differences from Prometheus before you port production queries. Refer to [Limitations](promql-limitations.md).

## Related pages

- [PromQL reference](/reference/query-languages/promql.md)
- [PromQL HTTP API](promql-http-api.md)
- [{{esql}} `PROMQL` command](/reference/query-languages/esql/commands/promql.md)
- [Ingest data with Prometheus remote write](docs-content://manage-data/data-store/data-streams/tsds-ingest-prometheus-remote-write.md)
- [Time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md)
