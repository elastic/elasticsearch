---
navigation_title: "Top metrics"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-top-metrics.html
---

# Top metrics aggregation [search-aggregations-metrics-top-metrics]


The `top_metrics` aggregation selects metrics from the document with the largest or smallest "sort" value. For example, this gets the value of the `m` field on the document with the largest value of `s`:

$$$search-aggregations-metrics-top-metrics-simple$$$

```console
POST /test/_bulk?refresh
{"index": {}}
{"s": 1, "m": 3.1415}
{"index": {}}
{"s": 2, "m": 1.0}
{"index": {}}
{"s": 3, "m": 2.71828}
POST /test/_search?filter_path=aggregations
{
  "aggs": {
    "tm": {
      "top_metrics": {
        "metrics": {"field": "m"},
        "sort": {"s": "desc"}
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "tm": {
      "top": [ {"sort": [3], "metrics": {"m": 2.718280076980591 } } ]
    }
  }
}
```

`top_metrics` is fairly similar to [`top_hits`](/reference/aggregations/search-aggregations-metrics-top-hits-aggregation.md) in spirit but because it is more limited it is able to do its job using less memory and is often faster.

## `sort` [_sort]

The `sort` field in the metric request functions exactly the same as the `sort` field in the [search](/reference/elasticsearch/rest-apis/sort-search-results.md) request except:

* It can’t be used on [binary](/reference/elasticsearch/mapping-reference/binary.md), [flattened](/reference/elasticsearch/mapping-reference/flattened.md), [ip](/reference/elasticsearch/mapping-reference/ip.md), [keyword](/reference/elasticsearch/mapping-reference/keyword.md), or [text](/reference/elasticsearch/mapping-reference/text.md) fields.
* It only supports a single sort value so which document wins ties is not specified.

The metrics that the aggregation returns is the first hit that would be returned by the search request. So,

`"sort": {"s": "desc"}`
:   gets metrics from the document with the highest `s`

`"sort": {"s": "asc"}`
:   gets the metrics from the document with the lowest `s`

`"sort": {"_geo_distance": {"location": "POINT (-78.6382 35.7796)"}}`
:   gets metrics from the documents with `location` **closest** to `35.7796, -78.6382`

`"sort": "_score"`
:   gets metrics from the document with the highest score


## `metrics` [_metrics]

`metrics` selects the fields of the "top" document to return. You can request a single metric with something like `"metrics": {"field": "m"}` or multiple metrics by requesting a list of metrics like `"metrics": [{"field": "m"}, {"field": "i"}`.

`metrics.field` supports the following field types:

* [`boolean`](/reference/elasticsearch/mapping-reference/boolean.md)
* [`ip`](/reference/elasticsearch/mapping-reference/ip.md)
* [keywords](/reference/elasticsearch/mapping-reference/keyword.md)
* [numbers](/reference/elasticsearch/mapping-reference/number.md)

Except for keywords, [runtime fields](docs-content://manage-data/data-store/mapping/runtime-fields.md) for corresponding types are also supported. `metrics.field` doesn’t support fields with [array values](/reference/elasticsearch/mapping-reference/array.md). A `top_metric` aggregation on array values may return inconsistent results.

The following example runs a `top_metrics` aggregation on several field types.

$$$search-aggregations-metrics-top-metrics-list-of-metrics$$$

```console
PUT /test
{
  "mappings": {
    "properties": {
      "d": {"type": "date"}
    }
  }
}
POST /test/_bulk?refresh
{"index": {}}
{"s": 1, "m": 3.1415, "i": 1, "d": "2020-01-01T00:12:12Z", "t": "cat"}
{"index": {}}
{"s": 2, "m": 1.0, "i": 6, "d": "2020-01-02T00:12:12Z", "t": "dog"}
{"index": {}}
{"s": 3, "m": 2.71828, "i": -12, "d": "2019-12-31T00:12:12Z", "t": "chicken"}
POST /test/_search?filter_path=aggregations
{
  "aggs": {
    "tm": {
      "top_metrics": {
        "metrics": [
          {"field": "m"},
          {"field": "i"},
          {"field": "d"},
          {"field": "t.keyword"}
        ],
        "sort": {"s": "desc"}
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "tm": {
      "top": [ {
        "sort": [3],
        "metrics": {
          "m": 2.718280076980591,
          "i": -12,
          "d": "2019-12-31T00:12:12.000Z",
          "t.keyword": "chicken"
        }
      } ]
    }
  }
}
```


## `missing` [_missing]

The `missing` parameter defines how documents with a missing value are treated. By default, if any of the key components are missing, the entire document is ignored. It is possible to treat the missing components as if they had a value by using the `missing` parameter.

```console
PUT /my-index
{
  "mappings": {
    "properties": {
      "nr":    { "type": "integer" },
      "state":  { "type": "keyword"  } <1>
    }
  }
}
POST /my-index/_bulk?refresh
{"index": {}}
{"nr": 1, "state": "started"}
{"index": {}}
{"nr": 2, "state": "stopped"}
{"index": {}}
{"nr": 3, "state": "N/A"}
{"index": {}}
{"nr": 4} <2>
POST /my-index/_search?filter_path=aggregations
{
  "aggs": {
    "my_top_metrics": {
      "top_metrics": {
        "metrics": {
          "field": "state",
          "missing": "N/A"}, <3>
        "sort": {"nr": "desc"}
      }
    }
  }
}
```

1. If you want to use an aggregation on textual content, it must be a `keyword` type field or you must enable fielddata on that field.
2. This document has a missing `state` field value.
3. The `missing` parameter defines that if `state` field has a missing value, it should be treated as if it had the `N/A` value.


The request results in the following response:

```console-result
{
  "aggregations": {
    "my_top_metrics": {
      "top": [
        {
          "sort": [
            4
          ],
          "metrics": {
            "state": "N/A"
          }
        }
      ]
    }
  }
}
```


## `size` [_size_2]

`top_metrics` can return the top few document’s worth of metrics using the size parameter:

$$$search-aggregations-metrics-top-metrics-size$$$

```console
POST /test/_bulk?refresh
{"index": {}}
{"s": 1, "m": 3.1415}
{"index": {}}
{"s": 2, "m": 1.0}
{"index": {}}
{"s": 3, "m": 2.71828}
POST /test/_search?filter_path=aggregations
{
  "aggs": {
    "tm": {
      "top_metrics": {
        "metrics": {"field": "m"},
        "sort": {"s": "desc"},
        "size": 3
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "tm": {
      "top": [
        {"sort": [3], "metrics": {"m": 2.718280076980591 } },
        {"sort": [2], "metrics": {"m": 1.0 } },
        {"sort": [1], "metrics": {"m": 3.1414999961853027 } }
      ]
    }
  }
}
```

The default `size` is 1. The maximum default size is `10` because the aggregation’s working storage is "dense", meaning we allocate `size` slots for every bucket. `10` is a **very** conservative default maximum and you can raise it if you need to by changing the `top_metrics_max_size` index setting. But know that large sizes can take a fair bit of memory, especially if they are inside of an aggregation which makes many buckes like a large [terms aggregation](#search-aggregations-metrics-top-metrics-example-terms). If you till want to raise it, use something like:

```console
PUT /test/_settings
{
  "top_metrics_max_size": 100
}
```

::::{note}
If `size` is more than `1` the `top_metrics` aggregation can’t be the **target** of a sort.
::::



## Examples [_examples_3]

### Use with terms [search-aggregations-metrics-top-metrics-example-terms]

This aggregation should be quite useful inside of [`terms`](/reference/aggregations/search-aggregations-bucket-terms-aggregation.md) aggregation, to, say, find the last value reported by each server.

$$$search-aggregations-metrics-top-metrics-terms$$$

```console
PUT /node
{
  "mappings": {
    "properties": {
      "ip": {"type": "ip"},
      "date": {"type": "date"}
    }
  }
}
POST /node/_bulk?refresh
{"index": {}}
{"ip": "192.168.0.1", "date": "2020-01-01T01:01:01", "m": 1}
{"index": {}}
{"ip": "192.168.0.1", "date": "2020-01-01T02:01:01", "m": 2}
{"index": {}}
{"ip": "192.168.0.2", "date": "2020-01-01T02:01:01", "m": 3}
POST /node/_search?filter_path=aggregations
{
  "aggs": {
    "ip": {
      "terms": {
        "field": "ip"
      },
      "aggs": {
        "tm": {
          "top_metrics": {
            "metrics": {"field": "m"},
            "sort": {"date": "desc"}
          }
        }
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "ip": {
      "buckets": [
        {
          "key": "192.168.0.1",
          "doc_count": 2,
          "tm": {
            "top": [ {"sort": ["2020-01-01T02:01:01.000Z"], "metrics": {"m": 2 } } ]
          }
        },
        {
          "key": "192.168.0.2",
          "doc_count": 1,
          "tm": {
            "top": [ {"sort": ["2020-01-01T02:01:01.000Z"], "metrics": {"m": 3 } } ]
          }
        }
      ],
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0
    }
  }
}
```

Unlike `top_hits`, you can sort buckets by the results of this metric:

```console
POST /node/_search?filter_path=aggregations
{
  "aggs": {
    "ip": {
      "terms": {
        "field": "ip",
        "order": {"tm.m": "desc"}
      },
      "aggs": {
        "tm": {
          "top_metrics": {
            "metrics": {"field": "m"},
            "sort": {"date": "desc"}
          }
        }
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "ip": {
      "buckets": [
        {
          "key": "192.168.0.2",
          "doc_count": 1,
          "tm": {
            "top": [ {"sort": ["2020-01-01T02:01:01.000Z"], "metrics": {"m": 3 } } ]
          }
        },
        {
          "key": "192.168.0.1",
          "doc_count": 2,
          "tm": {
            "top": [ {"sort": ["2020-01-01T02:01:01.000Z"], "metrics": {"m": 2 } } ]
          }
        }
      ],
      "doc_count_error_upper_bound": 0,
      "sum_other_doc_count": 0
    }
  }
}
```


### Mixed sort types [_mixed_sort_types]

Sorting `top_metrics` by a field that has different types across different indices producs somewhat surprising results: floating point fields are always sorted independently of whole numbered fields.

$$$search-aggregations-metrics-top-metrics-mixed-sort$$$

```console
POST /test/_bulk?refresh
{"index": {"_index": "test1"}}
{"s": 1, "m": 3.1415}
{"index": {"_index": "test1"}}
{"s": 2, "m": 1}
{"index": {"_index": "test2"}}
{"s": 3.1, "m": 2.71828}
POST /test*/_search?filter_path=aggregations
{
  "aggs": {
    "tm": {
      "top_metrics": {
        "metrics": {"field": "m"},
        "sort": {"s": "asc"}
      }
    }
  }
}
```

Which returns:

```js
{
  "aggregations": {
    "tm": {
      "top": [ {"sort": [3.0999999046325684], "metrics": {"m": 2.718280076980591 } } ]
    }
  }
}
```

While this is better than an error it **probably** isn’t what you were going for. While it does lose some precision, you can explicitly cast the whole number fields to floating points with something like:

```console
POST /test*/_search?filter_path=aggregations
{
  "aggs": {
    "tm": {
      "top_metrics": {
        "metrics": {"field": "m"},
        "sort": {"s": {"order": "asc", "numeric_type": "double"}}
      }
    }
  }
}
```

Which returns the much more expected:

```js
{
  "aggregations": {
    "tm": {
      "top": [ {"sort": [1.0], "metrics": {"m": 3.1414999961853027 } } ]
    }
  }
}
```


### Use in pipeline aggregations [_use_in_pipeline_aggregations_2]

`top_metrics` can be used in pipeline aggregations that consume a single value per bucket, such as `bucket_selector` that applies per bucket filtering, similar to using a HAVING clause in SQL. This requires setting `size` to 1, and specifying the right path for the (single) metric to be passed to the wrapping aggregator. For example:

```console
POST /test*/_search?filter_path=aggregations
{
  "aggs": {
    "ip": {
      "terms": {
        "field": "ip"
      },
      "aggs": {
        "tm": {
          "top_metrics": {
            "metrics": {"field": "m"},
            "sort": {"s": "desc"},
            "size": 1
          }
        },
        "having_tm": {
          "bucket_selector": {
            "buckets_path": {
              "top_m": "tm[m]"
            },
            "script": "params.top_m < 1000"
          }
        }
      }
    }
  }
}
```

The `bucket_path` uses the `top_metrics` name `tm` and a keyword for the metric providing the aggregate value, namely `m`.



