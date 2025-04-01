---
navigation_title: "Average bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-avg-bucket-aggregation.html
---

# Average bucket aggregation [search-aggregations-pipeline-avg-bucket-aggregation]


A sibling pipeline aggregation which calculates the mean value of a specified metric in a sibling aggregation. The specified metric must be numeric and the sibling aggregation must be a multi-bucket aggregation.

## Syntax [avg-bucket-agg-syntax]

```js
"avg_bucket": {
  "buckets_path": "sales_per_month>sales",
  "gap_policy": "skip",
  "format": "#,##0.00;(#,##0.00)"
}
```


## Parameters [avg-bucket-params]

`buckets_path`
:   (Required, string) Path to the buckets to average. For syntax, see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax).

`gap_policy`
:   (Optional, string) Policy to apply when gaps are found in the data. For valid values, see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy). Defaults to `skip`.

`format`
:   (Optional, string) [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html) for the output value. If specified, the formatted value is returned in the aggregation’s `value_as_string` property.


## Response body [avg-bucket-agg-response]

`value`
:   (float) Mean average value for the metric specified in `buckets_path`.

`value_as_string`
:   (string) Formatted output value for the aggregation. This property is only provided if a `format` is specified in the request.


## Example [avg-bucket-agg-ex]

The following `avg_monthly_sales` aggregation uses `avg_bucket` to calculate average sales per month:

```console
POST _search
{
  "size": 0,
  "aggs": {
    "sales_per_month": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"
      },
      "aggs": {
        "sales": {
          "sum": {
            "field": "price"
          }
        }
      }
    },
    "avg_monthly_sales": {
// tag::avg-bucket-agg-syntax[]               <1>
      "avg_bucket": {
        "buckets_path": "sales_per_month>sales",
        "gap_policy": "skip",
        "format": "#,##0.00;(#,##0.00)"
      }
// end::avg-bucket-agg-syntax[]               <2>
    }
  }
}
```

1. Start of the `avg_bucket` configuration. Comment is not part of the example.
2. End of the `avg_bucket` configuration. Comment is not part of the example.


The request returns the following response:

```console-result
{
  "took": 11,
  "timed_out": false,
  "_shards": ...,
  "hits": ...,
  "aggregations": {
    "sales_per_month": {
      "buckets": [
        {
          "key_as_string": "2015/01/01 00:00:00",
          "key": 1420070400000,
          "doc_count": 3,
          "sales": {
            "value": 550.0
          }
        },
        {
          "key_as_string": "2015/02/01 00:00:00",
          "key": 1422748800000,
          "doc_count": 2,
          "sales": {
            "value": 60.0
          }
        },
        {
          "key_as_string": "2015/03/01 00:00:00",
          "key": 1425168000000,
          "doc_count": 2,
          "sales": {
            "value": 375.0
          }
        }
      ]
    },
    "avg_monthly_sales": {
      "value": 328.33333333333333,
      "value_as_string": "328.33"
    }
  }
}
```


