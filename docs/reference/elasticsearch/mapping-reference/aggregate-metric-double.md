---
navigation_title: "Aggregate metric"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/aggregate-metric-double.html
---

# Aggregate metric field type [aggregate-metric-double]


Stores pre-aggregated numeric values for [metric aggregations](/reference/aggregations/metrics.md). An `aggregate_metric_double` field is an object containing one or more of the following metric sub-fields: `min`, `max`, `sum`, and `value_count`.

When you run certain metric aggregations on an `aggregate_metric_double` field, the aggregation uses the related sub-field’s values. For example, a [`min`](/reference/aggregations/search-aggregations-metrics-min-aggregation.md) aggregation on an `aggregate_metric_double` field returns the minimum value of all `min` sub-fields.

::::{important}
An `aggregate_metric_double` field stores a single numeric [doc value](/reference/elasticsearch/mapping-reference/doc-values.md) for each metric sub-field. Array values are not supported. `min`, `max`, and `sum` values are `double` numbers. `value_count` is a positive `long` number.
::::


```console
PUT my-index
{
  "mappings": {
    "properties": {
      "my-agg-metric-field": {
        "type": "aggregate_metric_double",
        "metrics": [ "min", "max", "sum", "value_count" ],
        "default_metric": "max"
      }
    }
  }
}
```

## Parameters for `aggregate_metric_double` fields [aggregate-metric-double-params]

`metrics`
:   (Required, array of strings) Array of metric sub-fields to store. Each value corresponds to a [metric aggregation](/reference/aggregations/metrics.md). Valid values are [`min`](/reference/aggregations/search-aggregations-metrics-min-aggregation.md), [`max`](/reference/aggregations/search-aggregations-metrics-max-aggregation.md), [`sum`](/reference/aggregations/search-aggregations-metrics-sum-aggregation.md), and [`value_count`](/reference/aggregations/search-aggregations-metrics-valuecount-aggregation.md). You must specify at least one value.

`default_metric`
:   (Required, string) Default metric sub-field to use for queries, scripts, and aggregations that don’t use a sub-field. Must be a value from the `metrics` array.

`time_series_metric`
:   (Optional, string) Marks the field as a [time series metric](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-metric). The value is the metric type. You can’t update this parameter for existing fields.

    **Valid `time_series_metric` values for `aggregate_metric_double` fields**:

    `gauge`
    :   A metric that represents a single numeric that can arbitrarily increase or decrease. For example, a temperature or available disk space.

    `null` (Default)
    :   Not a time series metric.


## Uses [aggregate-metric-double-uses]

We designed `aggregate_metric_double` fields for use with the following aggregations:

* A [`min`](/reference/aggregations/search-aggregations-metrics-min-aggregation.md) aggregation returns the minimum value of all `min` sub-fields.
* A [`max`](/reference/aggregations/search-aggregations-metrics-max-aggregation.md) aggregation returns the maximum value of all `max` sub-fields.
* A [`sum`](/reference/aggregations/search-aggregations-metrics-sum-aggregation.md) aggregation returns the sum of the values of all `sum` sub-fields.
* A [`value_count`](/reference/aggregations/search-aggregations-metrics-valuecount-aggregation.md) aggregation returns the sum of the values of all `value_count` sub-fields.
* A [`avg`](/reference/aggregations/search-aggregations-metrics-avg-aggregation.md) aggregation. There is no `avg` sub-field; the result of the `avg` aggregation is computed using the `sum` and `value_count` metrics. To run an `avg` aggregation, the field must contain both `sum` and `value_count` metric sub-field.

Running any other aggregation on an `aggregate_metric_double` field will fail with an "unsupported aggregation" error.

Finally, an `aggregate_metric_double` field supports the following queries for which it behaves as a `double` by delegating its behavior to its `default_metric` sub-field:

* [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md)
* [`range`](/reference/query-languages/query-dsl/query-dsl-range-query.md)
* [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md)
* [`terms`](/reference/query-languages/query-dsl/query-dsl-terms-query.md)


## Examples [aggregate-metric-double-example]

The following [create index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create) API request creates an index with an `aggregate_metric_double` field named `agg_metric`. The request sets `max` as the field’s `default_metric`.

```console
PUT stats-index
{
  "mappings": {
    "properties": {
      "agg_metric": {
        "type": "aggregate_metric_double",
        "metrics": [ "min", "max", "sum", "value_count" ],
        "default_metric": "max"
      }
    }
  }
}
```

The following [index](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create) API request adds documents with pre-aggregated data in the `agg_metric` field.

```console
PUT stats-index/_doc/1
{
  "agg_metric": {
    "min": -302.50,
    "max": 702.30,
    "sum": 200.0,
    "value_count": 25
  }
}

PUT stats-index/_doc/2
{
  "agg_metric": {
    "min": -93.00,
    "max": 1702.30,
    "sum": 300.00,
    "value_count": 25
  }
}
```
% TEST[continued]
% TEST[s/_doc\/2/_doc\/2?refresh=wait_for/]

You can run `min`, `max`, `sum`, `value_count`, and `avg` aggregations on a `agg_metric` field.

```console
POST stats-index/_search?size=0
{
  "aggs": {
    "metric_min": { "min": { "field": "agg_metric" } },
    "metric_max": { "max": { "field": "agg_metric" } },
    "metric_value_count": { "value_count": { "field": "agg_metric" } },
    "metric_sum": { "sum": { "field": "agg_metric" } },
    "metric_avg": { "avg": { "field": "agg_metric" } }
  }
}
```
% TEST[continued]

The aggregation results are based on related metric sub-field values.

```console-result
{
...
  "aggregations": {
    "metric_min": {
      "value": -302.5
    },
    "metric_max": {
      "value": 1702.3
    },
    "metric_value_count": {
      "value": 50
    },
    "metric_sum": {
      "value": 500.0
    },
    "metric_avg": {
      "value": 10.0
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

Queries on a `aggregate_metric_double` field use the `default_metric` value.

```console
GET stats-index/_search
{
  "query": {
    "term": {
      "agg_metric": {
        "value": 702.30
      }
    }
  }
}
```
% TEST[continued]

The search returns the following hit. The value of the `default_metric` field, `max`, matches the query value.

```console-result
{
  ...
    "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 1.0,
    "hits": [
      {
        "_index": "stats-index",
        "_id": "1",
        "_score": 1.0,
        "_source": {
          "agg_metric": {
            "min": -302.5,
            "max": 702.3,
            "sum": 200.0,
            "value_count": 25
          }
        }
      }
    ]
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,/]


## Synthetic `_source` [aggregate-metric-double-synthetic-source]

::::{important}
Synthetic `_source` is Generally Available only for TSDB indices (indices that have `index.mode` set to `time_series`). For other indices synthetic `_source` is in technical preview. Features in technical preview may be changed or removed in a future release. Elastic will work to fix any issues, but features in technical preview are not subject to the support SLA of official GA features.
::::


For example:

$$$synthetic-source-aggregate-metric-double-example$$$

```console
PUT idx
{
  "settings": {
    "index": {
      "mapping": {
        "source": {
          "mode": "synthetic"
        }
      }
    }
  },
  "mappings": {
    "properties": {
      "agg_metric": {
        "type": "aggregate_metric_double",
        "metrics": [ "min", "max", "sum", "value_count" ],
        "default_metric": "max"
      }
    }
  }
}

PUT idx/_doc/1
{
  "agg_metric": {
    "min": -302.50,
    "max": 702.30,
    "sum": 200.0,
    "value_count": 25
  }
}
```
% TEST[s/$/\nGET idx\/_doc\/1?filter_path=_source\n/]

Will become:

```console-result
{
  "agg_metric": {
    "min": -302.50,
    "max": 702.30,
    "sum": 200.0,
    "value_count": 25
  }
}
```
% TEST[s/^/{"_source":/ s/\n$/}/]


