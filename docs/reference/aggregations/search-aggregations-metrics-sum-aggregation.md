---
navigation_title: "Sum"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-sum-aggregation.html
---

# Sum aggregation [search-aggregations-metrics-sum-aggregation]


A `single-value` metrics aggregation that sums up numeric values that are extracted from the aggregated documents. These values can be extracted either from specific numeric or [histogram](/reference/elasticsearch/mapping-reference/histogram.md) fields.

Assuming the data consists of documents representing sales records we can sum the sale price of all hats with:

```console
POST /sales/_search?size=0
{
  "query": {
    "constant_score": {
      "filter": {
        "match": { "type": "hat" }
      }
    }
  },
  "aggs": {
    "hat_prices": { "sum": { "field": "price" } }
  }
}
```

Resulting in:

```console-result
{
  ...
  "aggregations": {
    "hat_prices": {
      "value": 450.0
    }
  }
}
```

The name of the aggregation (`hat_prices` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Script [_script_14]

If you need to get the `sum` for something more complex than a single field, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
POST /sales/_search?size=0
{
  "runtime_mappings": {
    "price.weighted": {
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
  "query": {
    "constant_score": {
      "filter": {
        "match": { "type": "hat" }
      }
    }
  },
  "aggs": {
    "hat_prices": {
      "sum": {
        "field": "price.weighted"
      }
    }
  }
}
```


## Missing value [_missing_value_17]

The `missing` parameter defines how documents that are missing a value should be treated. By default documents missing the value will be ignored but it is also possible to treat them as if they had a value. For example, this treats all hat sales without a price as being `100`.

```console
POST /sales/_search?size=0
{
  "query": {
    "constant_score": {
      "filter": {
        "match": { "type": "hat" }
      }
    }
  },
  "aggs": {
    "hat_prices": {
      "sum": {
        "field": "price",
        "missing": 100
      }
    }
  }
}
```


## Histogram fields [search-aggregations-metrics-sum-aggregation-histogram-fields]

When sum is computed on [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md), the result of the aggregation is the sum of all elements in the `values` array multiplied by the number in the same position in the `counts` array.

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
    "total_latency" : { "sum" : { "field" : "latency_histo" } }
  }
}
```

For each histogram field, the `sum` aggregation will add each number in the `values` array, multiplied by its associated count in the `counts` array.

Eventually, it will add all values for all histograms and return the following result:

```console-result
{
  "aggregations": {
    "total_latency": {
      "value": 28.8
    }
  }
}
```


