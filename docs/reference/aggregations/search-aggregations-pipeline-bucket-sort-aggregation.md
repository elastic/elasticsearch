---
navigation_title: "Bucket sort"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-sort-aggregation.html
---

# Bucket sort aggregation [search-aggregations-pipeline-bucket-sort-aggregation]


A parent pipeline aggregation which sorts the buckets of its parent multi-bucket aggregation. Zero or more sort fields may be specified together with the corresponding sort order. Each bucket may be sorted based on its `_key`, `_count` or its sub-aggregations. In addition, parameters `from` and `size` may be set in order to truncate the result buckets.

::::{note}
The `bucket_sort` aggregation, like all pipeline aggregations, is executed after all other non-pipeline aggregations. This means the sorting only applies to whatever buckets are already returned from the parent aggregation. For example, if the parent aggregation is `terms` and its `size` is set to `10`, the `bucket_sort` will only sort over those 10 returned term buckets.
::::


## Syntax [_syntax_10]

A `bucket_sort` aggregation looks like this in isolation:

```js
{
  "bucket_sort": {
    "sort": [
      { "sort_field_1": { "order": "asc" } },   <1>
      { "sort_field_2": { "order": "desc" } },
      "sort_field_3"
    ],
    "from": 1,
    "size": 3
  }
}
```
% NOTCONSOLE

1. Here, `sort_field_1` is the bucket path to the variable to be used as the primary sort and its order is ascending.


$$$bucket-sort-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `sort` | The list of fields to sort on. See [`sort`](/reference/elasticsearch/rest-apis/sort-search-results.md) for more details. | Optional |  |
| `from` | Buckets in positions prior to the set value will be truncated. | Optional | `0` |
| `size` | The number of buckets to return. Defaults to all buckets of the parent aggregation. | Optional |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |

The following snippet returns the buckets corresponding to the 3 months with the highest total sales in descending order:

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
        "total_sales": {
          "sum": {
            "field": "price"
          }
        },
        "sales_bucket_sort": {
          "bucket_sort": {
            "sort": [
              { "total_sales": { "order": "desc" } } <1>
            ],
            "size": 3                                <2>
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

1. `sort` is set to use the values of `total_sales` in descending order
2. `size` is set to `3` meaning only the top 3 months in `total_sales` will be returned


And the following may be the response:

```console-result
{
   "took": 82,
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
               "total_sales": {
                   "value": 550.0
               }
            },
            {
               "key_as_string": "2015/03/01 00:00:00",
               "key": 1425168000000,
               "doc_count": 2,
               "total_sales": {
                   "value": 375.0
               }
            },
            {
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2,
               "total_sales": {
                   "value": 60.0
               }
            }
         ]
      }
   }
}
```
% TESTRESPONSE[s/"took": 82/"took": $body.took/]
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": $body._shards/]
% TESTRESPONSE[s/"hits": \.\.\./"hits": $body.hits/]


## Truncating without sorting [_truncating_without_sorting]

It is also possible to use this aggregation in order to truncate the result buckets without doing any sorting. To do so, just use the `from` and/or `size` parameters without specifying `sort`.

The following example simply truncates the result so that only the second bucket is returned:

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
        "bucket_truncate": {
          "bucket_sort": {
            "from": 1,
            "size": 1
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

Response:

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
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2
            }
         ]
      }
   }
}
```
% TESTRESPONSE[s/"took": 11/"took": $body.took/]
% TESTRESPONSE[s/"_shards": \.\.\./"_shards": $body._shards/]
% TESTRESPONSE[s/"hits": \.\.\./"hits": $body.hits/]


