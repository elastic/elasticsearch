---
navigation_title: "Derivative"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-pipeline-derivative-aggregation.html
---

# Derivative aggregation [search-aggregations-pipeline-derivative-aggregation]


A parent pipeline aggregation which calculates the derivative of a specified metric in a parent histogram (or date_histogram) aggregation. The specified metric must be numeric and the enclosing histogram must have `min_doc_count` set to `0` (default for `histogram` aggregations).

## Syntax [_syntax_14]

A `derivative` aggregation looks like this in isolation:

```js
"derivative": {
  "buckets_path": "the_sum"
}
```

$$$derivative-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `buckets_path` | The path to the buckets we wish to find the derivative for (see [`buckets_path` Syntax](/reference/data-analysis/aggregations/pipeline.md#buckets-path-syntax) for more details) | Required |  |
| `gap_policy` | The policy to apply when gaps are found in the data (see [Dealing with gaps in the data](/reference/data-analysis/aggregations/pipeline.md#gap-policy) for more details) | Optional | `skip` |
| `format` | [DecimalFormat pattern](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/text/DecimalFormat.md) for theoutput value. If specified, the formatted value is returned in the aggregation’s`value_as_string` property | Optional | `null` |


## First Order Derivative [_first_order_derivative]

The following snippet calculates the derivative of the total monthly `sales`:

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
        },
        "sales_deriv": {
          "derivative": {
            "buckets_path": "sales" <1>
          }
        }
      }
    }
  }
}
```

1. `buckets_path` instructs this derivative aggregation to use the output of the `sales` aggregation for the derivative


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
               } <1>
            },
            {
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2,
               "sales": {
                  "value": 60.0
               },
               "sales_deriv": {
                  "value": -490.0 <2>
               }
            },
            {
               "key_as_string": "2015/03/01 00:00:00",
               "key": 1425168000000,
               "doc_count": 2, <3>
               "sales": {
                  "value": 375.0
               },
               "sales_deriv": {
                  "value": 315.0
               }
            }
         ]
      }
   }
}
```

1. No derivative for the first bucket since we need at least 2 data points to calculate the derivative
2. Derivative value units are implicitly defined by the `sales` aggregation and the parent histogram so in this case the units would be $/month assuming the `price` field has units of $.
3. The number of documents in the bucket are represented by the `doc_count`



## Second Order Derivative [_second_order_derivative]

A second order derivative can be calculated by chaining the derivative pipeline aggregation onto the result of another derivative pipeline aggregation as in the following example which will calculate both the first and the second order derivative of the total monthly sales:

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
        },
        "sales_deriv": {
          "derivative": {
            "buckets_path": "sales"
          }
        },
        "sales_2nd_deriv": {
          "derivative": {
            "buckets_path": "sales_deriv" <1>
          }
        }
      }
    }
  }
}
```

1. `buckets_path` for the second derivative points to the name of the first derivative


And the following may be the response:

```console-result
{
   "took": 50,
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
               } <1>
            },
            {
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2,
               "sales": {
                  "value": 60.0
               },
               "sales_deriv": {
                  "value": -490.0
               } <1>
            },
            {
               "key_as_string": "2015/03/01 00:00:00",
               "key": 1425168000000,
               "doc_count": 2,
               "sales": {
                  "value": 375.0
               },
               "sales_deriv": {
                  "value": 315.0
               },
               "sales_2nd_deriv": {
                  "value": 805.0
               }
            }
         ]
      }
   }
}
```

1. No second derivative for the first two buckets since we need at least 2 data points from the first derivative to calculate the second derivative



## Units [_units]

The derivative aggregation allows the units of the derivative values to be specified. This returns an extra field in the response `normalized_value` which reports the derivative value in the desired x-axis units. In the below example we calculate the derivative of the total sales per month but ask for the derivative of the sales as in the units of sales per day:

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
        },
        "sales_deriv": {
          "derivative": {
            "buckets_path": "sales",
            "unit": "day" <1>
          }
        }
      }
    }
  }
}
```

1. `unit` specifies what unit to use for the x-axis of the derivative calculation


And the following may be the response:

```console-result
{
   "took": 50,
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
               } <1>
            },
            {
               "key_as_string": "2015/02/01 00:00:00",
               "key": 1422748800000,
               "doc_count": 2,
               "sales": {
                  "value": 60.0
               },
               "sales_deriv": {
                  "value": -490.0, <1>
                  "normalized_value": -15.806451612903226 <2>
               }
            },
            {
               "key_as_string": "2015/03/01 00:00:00",
               "key": 1425168000000,
               "doc_count": 2,
               "sales": {
                  "value": 375.0
               },
               "sales_deriv": {
                  "value": 315.0,
                  "normalized_value": 11.25
               }
            }
         ]
      }
   }
}
```

1. `value` is reported in the original units of *per month*
2. `normalized_value` is reported in the desired units of *per day*



