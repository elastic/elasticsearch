---
navigation_title: "Avg"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-avg-aggregation.html
---

# Avg aggregation [search-aggregations-metrics-avg-aggregation]


A `single-value` metrics aggregation that computes the average of numeric values that are extracted from the aggregated documents. These values can be extracted either from specific numeric or [histogram](/reference/elasticsearch/mapping-reference/histogram.md) fields in the documents.

Assuming the data consists of documents representing exams grades (between 0 and 100) of students we can average their scores with:

```console
POST /exams/_search?size=0
{
  "aggs": {
    "avg_grade": { "avg": { "field": "grade" } }
  }
}
```
% TEST[setup:exams]

The above aggregation computes the average grade over all documents. The aggregation type is `avg` and the `field` setting defines the numeric field of the documents the average will be computed on. The above will return the following:

```console-result
{
  ...
  "aggregations": {
    "avg_grade": {
      "value": 75.0
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

The name of the aggregation (`avg_grade` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Script [_script_2]

Let’s say the exam was exceedingly difficult, and you need to apply a grade correction. Average a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md) to get a corrected average:

```console
POST /exams/_search?size=0
{
  "runtime_mappings": {
    "grade.corrected": {
      "type": "double",
      "script": {
        "source": "emit(Math.min(100, doc['grade'].value * params.correction))",
        "params": {
          "correction": 1.2
        }
      }
    }
  },
  "aggs": {
    "avg_corrected_grade": {
      "avg": {
        "field": "grade.corrected"
      }
    }
  }
}
```
% TEST[setup:exams]
% TEST[s/size=0/size=0&filter_path=aggregations/]


## Missing value [_missing_value_6]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
POST /exams/_search?size=0
{
  "aggs": {
    "grade_avg": {
      "avg": {
        "field": "grade",
        "missing": 10     <1>
      }
    }
  }
}
```
% TEST[setup:exams]

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `10`.



## Histogram fields [search-aggregations-metrics-avg-aggregation-histogram-fields]

When average is computed on [histogram fields](/reference/elasticsearch/mapping-reference/histogram.md), the result of the aggregation is the weighted average of all elements in the `values` array taking into consideration the number in the same position in the `counts` array.

For example, for the following index that stores pre-aggregated histograms with latency metrics for different networks:

```console
PUT metrics_index/_doc/1
{
  "network.name" : "net-1",
  "latency_histo" : {
      "values" : [0.1, 0.2, 0.3, 0.4, 0.5],
      "counts" : [3, 7, 23, 12, 6]
   }
}

PUT metrics_index/_doc/2
{
  "network.name" : "net-2",
  "latency_histo" : {
      "values" :  [0.1, 0.2, 0.3, 0.4, 0.5],
      "counts" : [8, 17, 8, 7, 6]
   }
}

POST /metrics_index/_search?size=0
{
  "aggs": {
    "avg_latency":
      { "avg": { "field": "latency_histo" }
    }
  }
}
```

For each histogram field the `avg` aggregation adds each number in the `values` array multiplied by its associated count in the `counts` array. Eventually, it will compute the average over those values for all histograms and return the following result:

```console-result
{
  ...
  "aggregations": {
    "avg_latency": {
      "value": 0.29690721649
    }
  }
}
```
% TESTRESPONSE[skip:test not setup]


