---
navigation_title: "Max bucket"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-max-bucket-aggregation.html
---

# Max bucket aggregation [search-aggregations-pipeline-max-bucket-aggregation]


A sibling pipeline aggregation which identifies the bucket(s) with the maximum value of a specified metric in a sibling aggregation and outputs both the value and the key(s) of the bucket(s). The specified metric must be numeric and the sibling aggregation must be a multi-bucket aggregation.

## Syntax [_syntax_16]

A `max_bucket` aggregation looks like this in isolation:

```js
{
  "max_bucket": {
    "buckets_path": "the_sum"
  }
}
```
% NOTCONSOLE

$$$max-bucket-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | The path to the buckets we wish to find the maximum for (see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |

The following snippet calculates the maximum of the total monthly `sales`:

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
    "max_monthly_sales": {
      "max_bucket": {
        "buckets_path": "sales_per_month>sales" <1>
      }
    }
  }
}
```
% TEST[setup:sales]

1. `buckets_path` instructs this max_bucket aggregation that we want the maximum value of the `sales` aggregation in the `sales_per_month` date histogram.


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
      "max_monthly_sales": {
          "keys": ["2015/01/01 00:00:00"], <1>
          "value": 550.0
      }
   }
}
```
% TESTRESPONSE[s/"took": 11/"took": $body.took/]
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": $body._shards/]
% TESTRESPONSE[s/"hits": \.\.\./"hits": $body.hits/]

1. `keys` is an array of strings since the maximum value may be present in multiple buckets



