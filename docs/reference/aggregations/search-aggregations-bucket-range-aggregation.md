---
navigation_title: "Range"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-range-aggregation.html
---

# Range aggregation [search-aggregations-bucket-range-aggregation]


A multi-bucket value source based aggregation that enables the user to define a set of ranges - each representing a bucket. During the aggregation process, the values extracted from each document will be checked against each bucket range and "bucket" the relevant/matching document. Note that this aggregation includes the `from` value and excludes the `to` value for each range.

Example:

$$$range-aggregation-example$$$

```console
GET sales/_search
{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          { "to": 100.0 },
          { "from": 100.0, "to": 200.0 },
          { "from": 200.0 }
        ]
      }
    }
  }
}
```
% TEST[setup:sales]
% TEST[s/_search/_search\?filter_path=aggregations/]

Response:

```console-result
{
  ...
  "aggregations": {
    "price_ranges": {
      "buckets": [
        {
          "key": "*-100.0",
          "to": 100.0,
          "doc_count": 2
        },
        {
          "key": "100.0-200.0",
          "from": 100.0,
          "to": 200.0,
          "doc_count": 2
        },
        {
          "key": "200.0-*",
          "from": 200.0,
          "doc_count": 3
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]

## Keyed Response [_keyed_response_4]

Setting the `keyed` flag to `true` will associate a unique string key with each bucket and return the ranges as a hash rather than an array:

$$$range-aggregation-keyed-example$$$

```console
GET sales/_search
{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "keyed": true,
        "ranges": [
          { "to": 100 },
          { "from": 100, "to": 200 },
          { "from": 200 }
        ]
      }
    }
  }
}
```
% TEST[setup:sales]
% TEST[s/_search/_search\?filter_path=aggregations/]

Response:

```console-result
{
  ...
  "aggregations": {
    "price_ranges": {
      "buckets": {
        "*-100.0": {
          "to": 100.0,
          "doc_count": 2
        },
        "100.0-200.0": {
          "from": 100.0,
          "to": 200.0,
          "doc_count": 2
        },
        "200.0-*": {
          "from": 200.0,
          "doc_count": 3
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]

It is also possible to customize the key for each range:

$$$range-aggregation-custom-keys-example$$$

```console
GET sales/_search
{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "keyed": true,
        "ranges": [
          { "key": "cheap", "to": 100 },
          { "key": "average", "from": 100, "to": 200 },
          { "key": "expensive", "from": 200 }
        ]
      }
    }
  }
}
```
% TEST[setup:sales]
% TEST[s/_search/_search\?filter_path=aggregations/]

Response:

```console-result
{
  ...
  "aggregations": {
    "price_ranges": {
      "buckets": {
        "cheap": {
          "to": 100.0,
          "doc_count": 2
        },
        "average": {
          "from": 100.0,
          "to": 200.0,
          "doc_count": 2
        },
        "expensive": {
          "from": 200.0,
          "doc_count": 3
        }
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]


## Script [_script]

If the data in your documents doesn’t exactly match what you’d like to aggregate, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). For example, if you need to apply a particular currency conversion rate:

$$$range-aggregation-runtime-field-example$$$

```console
GET sales/_search
{
  "runtime_mappings": {
    "price.euros": {
      "type": "double",
      "script": {
        "source": """
          emit(doc['price'].value * params.conversion_rate)
        """,
        "params": {
          "conversion_rate": 0.835526591
        }
      }
    }
  },
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price.euros",
        "ranges": [
          { "to": 100 },
          { "from": 100, "to": 200 },
          { "from": 200 }
        ]
      }
    }
  }
}
```
% TEST[setup:sales]
% TEST[s/_search/_search\?filter_path=aggregations/]


## Sub Aggregations [_sub_aggregations_2]

The following example, not only "bucket" the documents to the different buckets but also computes statistics over the prices in each price range

$$$range-aggregation-sub-aggregation-example$$$

```console
GET sales/_search
{
  "aggs": {
    "price_ranges": {
      "range": {
        "field": "price",
        "ranges": [
          { "to": 100 },
          { "from": 100, "to": 200 },
          { "from": 200 }
        ]
      },
      "aggs": {
        "price_stats": {
          "stats": { "field": "price" }
        }
      }
    }
  }
}
```
% TEST[setup:sales]
% TEST[s/_search/_search\?filter_path=aggregations/]

Response:

```console-result
{
  ...
  "aggregations": {
    "price_ranges": {
      "buckets": [
        {
          "key": "*-100.0",
          "to": 100.0,
          "doc_count": 2,
          "price_stats": {
            "count": 2,
            "min": 10.0,
            "max": 50.0,
            "avg": 30.0,
            "sum": 60.0
          }
        },
        {
          "key": "100.0-200.0",
          "from": 100.0,
          "to": 200.0,
          "doc_count": 2,
          "price_stats": {
            "count": 2,
            "min": 150.0,
            "max": 175.0,
            "avg": 162.5,
            "sum": 325.0
          }
        },
        {
          "key": "200.0-*",
          "from": 200.0,
          "doc_count": 3,
          "price_stats": {
            "count": 3,
            "min": 200.0,
            "max": 200.0,
            "avg": 200.0,
            "sum": 600.0
          }
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\.//]


## Histogram fields [search-aggregations-bucket-range-aggregation-histogram-fields]

Running a range aggregation over histogram fields computes the total number of counts for each configured range.

This is done without interpolating between the histogram field values. Consequently, it is possible to have a range that is "in-between" two histogram values. The resulting range bucket would have a zero doc count.

Here is an example, executing a range aggregation against the following index that stores pre-aggregated histograms with latency metrics (in milliseconds) for different networks:

```console
PUT metrics_index
{
  "mappings": {
    "properties": {
      "network": {
        "properties": {
          "name": {
            "type": "keyword"
          }
        }
      },
      "latency_histo": {
         "type": "histogram"
      }
    }
  }
}

PUT metrics_index/_doc/1?refresh
{
  "network.name" : "net-1",
  "latency_histo" : {
      "values" : [1, 3, 8, 12, 15],
      "counts" : [3, 7, 23, 12, 6]
   }
}

PUT metrics_index/_doc/2?refresh
{
  "network.name" : "net-2",
  "latency_histo" : {
      "values" : [1, 6, 8, 12, 14],
      "counts" : [8, 17, 8, 7, 6]
   }
}

GET metrics_index/_search?size=0&filter_path=aggregations
{
  "aggs": {
    "latency_ranges": {
      "range": {
        "field": "latency_histo",
        "ranges": [
          {"to": 2},
          {"from": 2, "to": 3},
          {"from": 3, "to": 10},
          {"from": 10}
        ]
      }
    }
  }
}
```

The `range` aggregation will sum the counts of each range computed based on the `values` and return the following output:

```console-result
{
  "aggregations": {
    "latency_ranges": {
      "buckets": [
        {
          "key": "*-2.0",
          "to": 2.0,
          "doc_count": 11
        },
        {
          "key": "2.0-3.0",
          "from": 2.0,
          "to": 3.0,
          "doc_count": 0
        },
        {
          "key": "3.0-10.0",
          "from": 3.0,
          "to": 10.0,
          "doc_count": 55
        },
        {
          "key": "10.0-*",
          "from": 10.0,
          "doc_count": 31
        }
      ]
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

::::{important}
Range aggregation is a bucket aggregation, which partitions documents into buckets rather than calculating metrics over fields like metrics aggregations do. Each bucket represents a collection of documents which sub-aggregations can run on. On the other hand, a histogram field is a pre-aggregated field representing multiple values inside a single field: buckets of numerical data and a count of items/documents for each bucket. This mismatch between the range aggregations expected input (expecting raw documents) and the histogram field (that provides summary information) limits the outcome of the aggregation to only the doc counts for each bucket.

**Consequently, when executing a range aggregation over a histogram field, no sub-aggregations are allowed.**

::::
