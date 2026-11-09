```console
PUT /promql-tutorial
{
  "settings": {
    "index": {
      "mode": "time_series", <1>
      "routing_path": ["labels.*"], <2>
      "time_series": {
        "start_time": "2026-10-22T11:00:00Z", <3>
        "end_time": "2026-10-22T13:00:00Z"
      }
    }
  },
  "mappings": {
    "dynamic_templates": [
      {
        "counter": {
          "path_match": ["metrics.*_total"], <4>
          "mapping": { "type": "double", "time_series_metric": "counter" }
        }
      }
    ],
    "properties": {
      "@timestamp": { "type": "date" },
      "labels": { "type": "passthrough", "priority": 10, "time_series_dimension": true }, <5>
      "metrics": { "type": "passthrough", "priority": 20 } <6>
    }
  }
}
```
1. PromQL queries only run against time series indices and [time series data streams](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md) (TSDS).
2. Route documents by their label dimensions.
3. Accept the fixed sample timestamps that this tutorial uses.
4. Map metrics with names that end in `_total`, such as `requests_total`, as counters.
5. Store each Prometheus label, including the `__name__` metric name label, as a time series dimension.
6. Expose each field under `metrics` by its short name, so `metrics.requests_total` is queryable as `requests_total`.
