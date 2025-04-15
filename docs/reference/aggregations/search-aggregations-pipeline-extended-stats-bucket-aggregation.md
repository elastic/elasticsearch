---
navigation_title: "Extended stats bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-extended-stats-bucket-aggregation.html
---

# Extended stats bucket aggregation [search-aggregations-pipeline-extended-stats-bucket-aggregation]


A sibling pipeline aggregation which calculates a variety of stats across all bucket of a specified metric in a sibling aggregation. The specified metric must be numeric and the sibling aggregation must be a multi-bucket aggregation.

This aggregation provides a few more statistics (sum of squares, standard deviation, etc) compared to the `stats_bucket` aggregation.

## Syntax [_syntax_15]

A `extended_stats_bucket` aggregation looks like this in isolation:

```js
{
  "extended_stats_bucket": {
    "buckets_path": "the_sum"
  }
}
```

$$$extended-stats-bucket-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | The path to the buckets we wish to calculate stats for (see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |
| `sigma` | The number of standard deviations above/below the mean to display | Optional | 2 |

The following snippet calculates the extended stats for monthly `sales` bucket:

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
      "extended_stats_bucket": {
        "buckets_path": "sales_per_month>sales" <1>
      }
    }
  }
}
```

1. `bucket_paths` instructs this `extended_stats_bucket` aggregation that we want the calculate stats for the `sales` aggregation in the `sales_per_month` date histogram.


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
         "sum": 985.0,
         "sum_of_squares": 446725.0,
         "variance": 41105.55555555556,
         "variance_population": 41105.55555555556,
         "variance_sampling": 61658.33333333334,
         "std_deviation": 202.74505063146563,
         "std_deviation_population": 202.74505063146563,
         "std_deviation_sampling": 248.3109609609156,
         "std_deviation_bounds": {
           "upper": 733.8234345962646,
           "lower": -77.15676792959795,
           "upper_population" : 733.8234345962646,
           "lower_population" : -77.15676792959795,
           "upper_sampling" : 824.9552552551645,
           "lower_sampling" : -168.28858858849787
         }
      }
   }
}
```


