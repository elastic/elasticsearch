---
navigation_title: "Bucket script"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-bucket-script-aggregation.html
---

# Bucket script aggregation [search-aggregations-pipeline-bucket-script-aggregation]


A parent pipeline aggregation which executes a script which can perform per bucket computations on specified metrics in the parent multi-bucket aggregation. The specified metric must be numeric and the script must return a numeric value.

## Syntax [bucket-script-agg-syntax]

A `bucket_script` aggregation looks like this in isolation:

```js
{
  "bucket_script": {
    "buckets_path": {
      "my_var1": "the_sum",                     <1>
      "my_var2": "the_value_count"
    },
    "script": "params.my_var1 / params.my_var2"
  }
}
```

1. Here, `my_var1` is the name of the variable for this buckets path to use in the script, `the_sum` is the path to the metrics to use for that variable.


$$$bucket-script-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `script` | The script to run for this aggregation. The script can be inline, file or indexed. (see [Scripting](docs-content://explore-analyze/scripting.md)for more details) | Required |  |
| `buckets_path` | A map of script variables and their associated path to the buckets we wish to use for the variable(see [`buckets_path` Syntax](/reference/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.html) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |

The following snippet calculates the ratio percentage of t-shirt sales compared to total sales each month:

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
        "t-shirts": {
          "filter": {
            "term": {
              "type": "t-shirt"
            }
          },
          "aggs": {
            "sales": {
              "sum": {
                "field": "price"
              }
            }
          }
        },
        "t-shirt-percentage": {
          "bucket_script": {
            "buckets_path": {
              "tShirtSales": "t-shirts>sales",
              "totalSales": "total_sales"
            },
            "script": "params.tShirtSales / params.totalSales * 100"
          }
        }
      }
    }
  }
}
```

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
               "total_sales": {
                   "value": 550.0
               },
               "t-shirts": {
                   "doc_count": 1,
                   "sales": {
                       "value": 200.0
                   }
               },
               "t-shirt-percentage": {
                   "value": 36.36363636363637
               }
            },
            {
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2,
               "total_sales": {
                   "value": 60.0
               },
               "t-shirts": {
                   "doc_count": 1,
                   "sales": {
                       "value": 10.0
                   }
               },
               "t-shirt-percentage": {
                   "value": 16.666666666666664
               }
            },
            {
               "key_as_string": "2015/03/01 00:00:00",
               "key": 1425168000000,
               "doc_count": 2,
               "total_sales": {
                   "value": 375.0
               },
               "t-shirts": {
                   "doc_count": 1,
                   "sales": {
                       "value": 175.0
                   }
               },
               "t-shirt-percentage": {
                   "value": 46.666666666666664
               }
            }
         ]
      }
   }
}
```


