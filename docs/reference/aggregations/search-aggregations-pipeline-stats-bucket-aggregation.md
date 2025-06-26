---
navigation_title: "Stats bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-stats-bucket-aggregation.html
---

# Stats bucket aggregation [search-aggregations-pipeline-stats-bucket-aggregation]


A sibling pipeline aggregation which calculates a variety of stats across all bucket of a specified metric in a sibling aggregation. The specified metric must be numeric and the sibling aggregation must be a multi-bucket aggregation.

## Syntax [_syntax_23]

A `stats_bucket` aggregation looks like this in isolation:

```js
{
  "stats_bucket": {
    "buckets_path": "the_sum"
  }
}
```

$$$stats-bucket-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | The path to the buckets we wish to calculate stats for (see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |

The following snippet calculates the stats for monthly `sales`:

```console
POST /sales/_search
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
    "stats_monthly_sales": {
      "stats_bucket": {
        "buckets_path": "sales_per_month>sales" <1>
      }
    }
  }
}
```

1. `bucket_paths` instructs this `stats_bucket` aggregation that we want the calculate stats for the `sales` aggregation in the `sales_per_month` date histogram.


And the following may be the response:

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
      "stats_monthly_sales": {
         "count": 3,
         "min": 60.0,
         "max": 550.0,
         "avg": 328.3333333333333,
         "sum": 985.0
      }
   }
}
```


