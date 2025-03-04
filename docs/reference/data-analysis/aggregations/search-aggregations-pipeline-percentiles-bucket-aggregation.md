---
navigation_title: "Percentiles bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-percentiles-bucket-aggregation.html
---

# Percentiles bucket aggregation [search-aggregations-pipeline-percentiles-bucket-aggregation]


A sibling pipeline aggregation which calculates percentiles across all bucket of a specified metric in a sibling aggregation. The specified metric must be numeric and the sibling aggregation must be a multi-bucket aggregation.

## Syntax [_syntax_21]

A `percentiles_bucket` aggregation looks like this in isolation:

```js
{
  "percentiles_bucket": {
    "buckets_path": "the_sum"
  }
}
```

$$$percentiles-bucket-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | The path to the buckets we wish to find the percentiles for (see [`buckets_path` Syntax](/reference/data-analysis/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/data-analysis/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.md) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |
| `percents` | The list of percentiles to calculate | Optional | `[ 1, 5, 25, 50, 75, 95, 99 ]` |
| `keyed` | Flag which returns the range as an hash instead of an array of key-value pairs | Optional | `true` |

The following snippet calculates the percentiles for the total monthly `sales` buckets:

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
    "percentiles_monthly_sales": {
      "percentiles_bucket": {
        "buckets_path": "sales_per_month>sales", <1>
        "percents": [ 25.0, 50.0, 75.0 ]         <2>
      }
    }
  }
}
```

1. `buckets_path` instructs this percentiles_bucket aggregation that we want to calculate percentiles for the `sales` aggregation in the `sales_per_month` date histogram.
2. `percents` specifies which percentiles we wish to calculate, in this case, the 25th, 50th and 75th percentiles.


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
      "percentiles_monthly_sales": {
        "values" : {
            "25.0": 375.0,
            "50.0": 375.0,
            "75.0": 550.0
         }
      }
   }
}
```


## Percentiles_bucket implementation [_percentiles_bucket_implementation]

The percentiles are calculated exactly and is not an approximation (unlike the Percentiles Metric). This means the implementation maintains an in-memory, sorted list of your data to compute the percentiles, before discarding the data. You may run into memory pressure issues if you attempt to calculate percentiles over many millions of data-points in a single `percentiles_bucket`.

The Percentile Bucket returns the nearest input data point to the requested percentile, rounding indices toward positive infinity; it does not interpolate between data points. For example, if there are eight data points and you request the `50%th` percentile, it will return the `4th` item because `ROUND_UP(.50 * (8-1))` is `4`.


