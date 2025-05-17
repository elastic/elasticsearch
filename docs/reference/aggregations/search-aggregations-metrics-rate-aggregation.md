---
navigation_title: "Rate"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-rate-aggregation.html
---

# Rate aggregation [search-aggregations-metrics-rate-aggregation]


A `rate` metrics aggregation can be used only inside a `date_histogram` or `composite` aggregation. It calculates a rate of documents or a field in each bucket. The field values can be extracted from specific numeric or [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md) in the documents.

::::{note}
For `composite` aggregations, there must be exactly one `date_histogram` source for the `rate` aggregation to be supported.
::::


## Syntax [_syntax_5]

A `rate` aggregation looks like this in isolation:

```js
{
  "rate": {
    "unit": "month",
    "field": "requests"
  }
}
```
% NOTCONSOLE

The following request will group all sales records into monthly buckets and then convert the number of sales transactions in each bucket into per annual sales rate.

```console
GET sales/_search
{
  "size": 0,
  "aggs": {
    "by_date": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"  <1>
      },
      "aggs": {
        "my_rate": {
          "rate": {
            "unit": "year"  <2>
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

1. Histogram is grouped by month.
2. But the rate is converted into annual rate.


The response will return the annual rate of transactions in each bucket. Since there are 12 months per year, the annual rate will be automatically calculated by multiplying the monthly rate by 12.

```console-result
{
  ...
  "aggregations" : {
    "by_date" : {
      "buckets" : [
        {
          "key_as_string" : "2015/01/01 00:00:00",
          "key" : 1420070400000,
          "doc_count" : 3,
          "my_rate" : {
            "value" : 36.0
          }
        },
        {
          "key_as_string" : "2015/02/01 00:00:00",
          "key" : 1422748800000,
          "doc_count" : 2,
          "my_rate" : {
            "value" : 24.0
          }
        },
        {
          "key_as_string" : "2015/03/01 00:00:00",
          "key" : 1425168000000,
          "doc_count" : 2,
          "my_rate" : {
            "value" : 24.0
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

Instead of counting the number of documents, it is also possible to calculate a sum of all values of the fields in the documents in each bucket or the number of values in each bucket. The following request will group all sales records into monthly bucket and than calculate the total monthly sales and convert them into average daily sales.

```console
GET sales/_search
{
  "size": 0,
  "aggs": {
    "by_date": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"  <1>
      },
      "aggs": {
        "avg_price": {
          "rate": {
            "field": "price", <2>
            "unit": "day"  <3>
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

1. Histogram is grouped by month.
2. Calculate sum of all sale prices
3. Convert to average daily sales


The response will contain the average daily sale prices for each month.

```console-result
{
  ...
  "aggregations" : {
    "by_date" : {
      "buckets" : [
        {
          "key_as_string" : "2015/01/01 00:00:00",
          "key" : 1420070400000,
          "doc_count" : 3,
          "avg_price" : {
            "value" : 17.741935483870968
          }
        },
        {
          "key_as_string" : "2015/02/01 00:00:00",
          "key" : 1422748800000,
          "doc_count" : 2,
          "avg_price" : {
            "value" : 2.142857142857143
          }
        },
        {
          "key_as_string" : "2015/03/01 00:00:00",
          "key" : 1425168000000,
          "doc_count" : 2,
          "avg_price" : {
            "value" : 12.096774193548388
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

You can also take advantage of `composite` aggregations to calculate the average daily sale price for each item in your inventory

```console
GET sales/_search?filter_path=aggregations&size=0
{
  "aggs": {
    "buckets": {
      "composite": { <1>
        "sources": [
          {
            "month": {
              "date_histogram": { <2>
                "field": "date",
                "calendar_interval": "month"
              }
            }
          },
          {
            "type": { <3>
              "terms": {
                "field": "type"
              }
            }
          }
        ]
      },
      "aggs": {
        "avg_price": {
          "rate": {
            "field": "price", <4>
            "unit": "day" <5>
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

1. Composite aggregation with a date histogram source and a source for the item type.
2. The date histogram source grouping monthly
3. The terms source grouping for each sale item type
4. Calculate sum of all sale prices, per month and item
5. Convert to average daily sales per item


The response will contain the average daily sale prices for each month per item.

```console-result
{
  "aggregations" : {
    "buckets" : {
      "after_key" : {
        "month" : 1425168000000,
        "type" : "t-shirt"
      },
      "buckets" : [
        {
          "key" : {
            "month" : 1420070400000,
            "type" : "bag"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 4.838709677419355
          }
        },
        {
          "key" : {
            "month" : 1420070400000,
            "type" : "hat"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 6.451612903225806
          }
        },
        {
          "key" : {
            "month" : 1420070400000,
            "type" : "t-shirt"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 6.451612903225806
          }
        },
        {
          "key" : {
            "month" : 1422748800000,
            "type" : "hat"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 1.7857142857142858
          }
        },
        {
          "key" : {
            "month" : 1422748800000,
            "type" : "t-shirt"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 0.35714285714285715
          }
        },
        {
          "key" : {
            "month" : 1425168000000,
            "type" : "hat"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 6.451612903225806
          }
        },
        {
          "key" : {
            "month" : 1425168000000,
            "type" : "t-shirt"
          },
          "doc_count" : 1,
          "avg_price" : {
            "value" : 5.645161290322581
          }
        }
      ]
    }
  }
}
```

By adding the `mode` parameter with the value `value_count`, we can change the calculation from `sum` to the number of values of the field:

```console
GET sales/_search
{
  "size": 0,
  "aggs": {
    "by_date": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"  <1>
      },
      "aggs": {
        "avg_number_of_sales_per_year": {
          "rate": {
            "field": "price", <2>
            "unit": "year",  <3>
            "mode": "value_count" <4>
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

1. Histogram is grouped by month.
2. Calculate number of all sale prices
3. Convert to annual counts
4. Changing the mode to value count


The response will contain the average daily sale prices for each month.

```console-result
{
  ...
  "aggregations" : {
    "by_date" : {
      "buckets" : [
        {
          "key_as_string" : "2015/01/01 00:00:00",
          "key" : 1420070400000,
          "doc_count" : 3,
          "avg_number_of_sales_per_year" : {
            "value" : 36.0
          }
        },
        {
          "key_as_string" : "2015/02/01 00:00:00",
          "key" : 1422748800000,
          "doc_count" : 2,
          "avg_number_of_sales_per_year" : {
            "value" : 24.0
          }
        },
        {
          "key_as_string" : "2015/03/01 00:00:00",
          "key" : 1425168000000,
          "doc_count" : 2,
          "avg_number_of_sales_per_year" : {
            "value" : 24.0
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

By default `sum` mode is used.

`"mode": "sum"`
:   calculate the sum of all values field

`"mode": "value_count"`
:   use the number of values in the field


## Relationship between bucket sizes and rate [_relationship_between_bucket_sizes_and_rate]

The `rate` aggregation supports all rate that can be used [calendar_intervals parameter](/reference/aggregations/search-aggregations-bucket-datehistogram-aggregation.md#calendar_intervals) of `date_histogram` aggregation. The specified rate should compatible with the `date_histogram` aggregation interval, i.e. it should be possible to convert the bucket size into the rate. By default the interval of the `date_histogram` is used.

`"rate": "second"`
:   compatible with all intervals

`"rate": "minute"`
:   compatible with all intervals

`"rate": "hour"`
:   compatible with all intervals

`"rate": "day"`
:   compatible with all intervals

`"rate": "week"`
:   compatible with all intervals

`"rate": "month"`
:   compatible with only with `month`, `quarter` and `year` calendar intervals

`"rate": "quarter"`
:   compatible with only with `month`, `quarter` and `year` calendar intervals

`"rate": "year"`
:   compatible with only with `month`, `quarter` and `year` calendar intervals

There is also an additional limitations if the date histogram is not a direct parent of the rate histogram. In this case both rate interval and histogram interval have to be in the same group: [`second`, ` minute`, `hour`, `day`, `week`] or [`month`, `quarter`, `year`]. For example, if the date histogram is `month` based, only rate intervals of `month`, `quarter` or `year` are supported. If the date histogram is `day` based, only  `second`, ` minute`, `hour`, `day`, and `week` rate intervals are supported.


## Script [_script_11]

If you need to run the aggregation against values that aren’t indexed, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). For example, if we need to adjust our prices before calculating rates:

```console
GET sales/_search
{
  "size": 0,
  "runtime_mappings": {
    "price.adjusted": {
      "type": "double",
      "script": {
        "source": "emit(doc['price'].value * params.adjustment)",
        "params": {
          "adjustment": 0.9
        }
      }
    }
  },
  "aggs": {
    "by_date": {
      "date_histogram": {
        "field": "date",
        "calendar_interval": "month"
      },
      "aggs": {
        "avg_price": {
          "rate": {
            "field": "price.adjusted"
          }
        }
      }
    }
  }
}
```
% TEST[setup:sales]

```console-result
{
  ...
  "aggregations" : {
    "by_date" : {
      "buckets" : [
        {
          "key_as_string" : "2015/01/01 00:00:00",
          "key" : 1420070400000,
          "doc_count" : 3,
          "avg_price" : {
            "value" : 495.0
          }
        },
        {
          "key_as_string" : "2015/02/01 00:00:00",
          "key" : 1422748800000,
          "doc_count" : 2,
          "avg_price" : {
            "value" : 54.0
          }
        },
        {
          "key_as_string" : "2015/03/01 00:00:00",
          "key" : 1425168000000,
          "doc_count" : 2,
          "avg_price" : {
            "value" : 337.5
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]


