---
navigation_title: "Weighted avg"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-weight-avg-aggregation.html
---

# Weighted avg aggregation [search-aggregations-metrics-weight-avg-aggregation]


A `single-value` metrics aggregation that computes the weighted average of numeric values that are extracted from the aggregated documents. These values can be extracted either from specific numeric fields in the documents.

When calculating a regular average, each datapoint has an equal "weight" …  it contributes equally to the final value. Weighted averages, on the other hand, weight each datapoint differently. The amount that each datapoint contributes to the final value is extracted from the document.

As a formula, a weighted average is the `∑(value * weight) / ∑(weight)`

A regular average can be thought of as a weighted average where every value has an implicit weight of `1`.

$$$weighted-avg-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `value` | The configuration for the field or script that provides the values | Required |  |
| `weight` | The configuration for the field or script that provides the weights | Required |  |
| `format` | The numeric response formatter | Optional |  |

The `value` and `weight` objects have per-field specific configuration:

$$$value-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `field` | The field that values should be extracted from | Required |  |
| `missing` | A value to use if the field is missing entirely | Optional |  |

$$$weight-params$$$

| Parameter Name | Description | Required | Default Value |
| --- | --- | --- | --- |
| `field` | The field that weights should be extracted from | Required |  |
| `missing` | A weight to use if the field is missing entirely | Optional |  |

## Examples [_examples_4]

If our documents have a `"grade"` field that holds a 0-100 numeric score, and a `"weight"` field which holds an arbitrary numeric weight, we can calculate the weighted average using:

```console
POST /exams/_search
{
  "size": 0,
  "aggs": {
    "weighted_grade": {
      "weighted_avg": {
        "value": {
          "field": "grade"
        },
        "weight": {
          "field": "weight"
        }
      }
    }
  }
}
```
% TEST[setup:exams]

Which yields a response like:

```console-result
{
  ...
  "aggregations": {
    "weighted_grade": {
      "value": 70.0
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

While multiple values-per-field are allowed, only one weight is allowed. If the aggregation encounters a document that has more than one weight (e.g. the weight field is a multi-valued field) it will abort the search. If you have this situation, you should build a [Runtime field](#search-aggregations-metrics-weight-avg-aggregation-runtime-field) to combine those values into a single weight.

This single weight will be applied independently to each value extracted from the `value` field.

This example show how a single document with multiple values will be averaged with a single weight:

```console
POST /exams/_doc?refresh
{
  "grade": [1, 2, 3],
  "weight": 2
}

POST /exams/_search
{
  "size": 0,
  "aggs": {
    "weighted_grade": {
      "weighted_avg": {
        "value": {
          "field": "grade"
        },
        "weight": {
          "field": "weight"
        }
      }
    }
  }
}
```
% TEST

The three values (`1`, `2`, and `3`) will be included as independent values, all with the weight of `2`:

```console-result
{
  ...
  "aggregations": {
    "weighted_grade": {
      "value": 2.0
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

The aggregation returns `2.0` as the result, which matches what we would expect when calculating by hand: `((1*2) + (2*2) + (3*2)) / (2+2+2) == 2`


## Runtime field [search-aggregations-metrics-weight-avg-aggregation-runtime-field]

If you have to sum or weigh values that don’t quite line up with the indexed values, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
POST /exams/_doc?refresh
{
  "grade": 100,
  "weight": [2, 3]
}
POST /exams/_doc?refresh
{
  "grade": 80,
  "weight": 3
}

POST /exams/_search?filter_path=aggregations
{
  "size": 0,
  "runtime_mappings": {
    "weight.combined": {
      "type": "double",
      "script": """
        double s = 0;
        for (double w : doc['weight']) {
          s += w;
        }
        emit(s);
      """
    }
  },
  "aggs": {
    "weighted_grade": {
      "weighted_avg": {
        "value": {
          "script": "doc.grade.value + 1"
        },
        "weight": {
          "field": "weight.combined"
        }
      }
    }
  }
}
```

Which should look like:

```console-result
{
  "aggregations": {
    "weighted_grade": {
      "value": 93.5
    }
  }
}
```


## Missing values [_missing_values_4]

By default, the aggregation excludes documents with a missing or `null` value for the `value` or `weight` field. Use the `missing` parameter to specify a default value for these documents instead.

```console
POST /exams/_search
{
  "size": 0,
  "aggs": {
    "weighted_grade": {
      "weighted_avg": {
        "value": {
          "field": "grade",
          "missing": 2
        },
        "weight": {
          "field": "weight",
          "missing": 3
        }
      }
    }
  }
}
```
% TEST[setup:exams]


