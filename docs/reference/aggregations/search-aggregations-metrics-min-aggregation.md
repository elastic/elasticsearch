---
navigation_title: "Min"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-min-aggregation.html
---

# Min aggregation [search-aggregations-metrics-min-aggregation]


A `single-value` metrics aggregation that keeps track and returns the minimum value among numeric values extracted from the aggregated documents.

::::{note}
The `min` and `max` aggregation operate on the `double` representation of the data. As a consequence, the result may be approximate when running on longs whose absolute value is greater than `2^53`.
::::


Computing the min price value across all documents:

```console
POST /sales/_search?size=0
{
  "aggs": {
    "min_price": { "min": { "field": "price" } }
  }
}
```

Response:

```console-result
{
  ...

  "aggregations": {
    "min_price": {
      "value": 10.0
    }
  }
}
```

As can be seen, the name of the aggregation (`min_price` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Script [_script_8]

If you need to get the `min` of something more complex than a single field, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
POST /sales/_search
{
  "size": 0,
  "runtime_mappings": {
    "price.adjusted": {
      "type": "double",
      "script": """
        double price = doc['price'].value;
        if (doc['promoted'].value) {
          price *= 0.8;
        }
        emit(price);
      """
    }
  },
  "aggs": {
    "min_price": {
      "min": { "field": "price.adjusted" }
    }
  }
}
```


## Missing value [_missing_value_12]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
POST /sales/_search
{
  "aggs": {
    "grade_min": {
      "min": {
        "field": "grade",
        "missing": 10 <1>
      }
    }
  }
}
```

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `10`.



## Histogram fields [search-aggregations-metrics-min-aggregation-histogram-fields]

When `min` is computed on [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md), the result of the aggregation is the minimum of all elements in the `values` array. Note, that the `counts` array of the histogram is ignored.

For example, for the following index that stores pre-aggregated histograms with latency metrics for different networks:

```console
PUT metrics_index
{
  "mappings": {
    "properties": {
      "latency_histo": { "type": "histogram" }
    }
  }
}

PUT metrics_index/_doc/1?refresh
{
  "network.name" : "net-1",
  "latency_histo" : {
      "values" : [0.1, 0.2, 0.3, 0.4, 0.5],
      "counts" : [3, 7, 23, 12, 6]
   }
}

PUT metrics_index/_doc/2?refresh
{
  "network.name" : "net-2",
  "latency_histo" : {
      "values" :  [0.1, 0.2, 0.3, 0.4, 0.5],
      "counts" : [8, 17, 8, 7, 6]
   }
}

POST /metrics_index/_search?size=0&filter_path=aggregations
{
  "aggs" : {
    "min_latency" : { "min" : { "field" : "latency_histo" } }
  }
}
```

The `min` aggregation will return the minimum value of all histogram fields:

```console-result
{
  "aggregations": {
    "min_latency": {
      "value": 0.1
    }
  }
}
```


