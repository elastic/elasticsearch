---
navigation_title: "Extended stats"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-extendedstats-aggregation.html
---

# Extended stats aggregation [search-aggregations-metrics-extendedstats-aggregation]


A `multi-value` metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

The `extended_stats` aggregations is an extended version of the [`stats`](/reference/aggregations/search-aggregations-metrics-stats-aggregation.md) aggregation, where additional metrics are added such as `sum_of_squares`, `variance`, `std_deviation` and `std_deviation_bounds`.

Assuming the data consists of documents representing exams grades (between 0 and 100) of students

```console
GET /exams/_search
{
  "size": 0,
  "aggs": {
    "grades_stats": { "extended_stats": { "field": "grade" } }
  }
}
```
% TEST[setup:exams]

The above aggregation computes the grades statistics over all documents. The aggregation type is `extended_stats` and the `field` setting defines the numeric field of the documents the stats will be computed on. The above will return the following:

The `std_deviation` and `variance` are calculated as population metrics so they are always the same as `std_deviation_population` and `variance_population` respectively.

```console-result
{
  ...

  "aggregations": {
    "grades_stats": {
      "count": 2,
      "min": 50.0,
      "max": 100.0,
      "avg": 75.0,
      "sum": 150.0,
      "sum_of_squares": 12500.0,
      "variance": 625.0,
      "variance_population": 625.0,
      "variance_sampling": 1250.0,
      "std_deviation": 25.0,
      "std_deviation_population": 25.0,
      "std_deviation_sampling": 35.35533905932738,
      "std_deviation_bounds": {
        "upper": 125.0,
        "lower": 25.0,
        "upper_population": 125.0,
        "lower_population": 25.0,
        "upper_sampling": 145.71067811865476,
        "lower_sampling": 4.289321881345245
      }
    }
  }
}
```
% TESTRESPONSE[s/\.\.\./"took": $body.took,"timed_out": false,"_shards": $body._shards,"hits": $body.hits,/]

The name of the aggregation (`grades_stats` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Standard Deviation Bounds [_standard_deviation_bounds]

By default, the `extended_stats` metric will return an object called `std_deviation_bounds`, which provides an interval of plus/minus two standard deviations from the mean. This can be a useful way to visualize variance of your data. If you want a different boundary, for example three standard deviations, you can set `sigma` in the request:

```console
GET /exams/_search
{
  "size": 0,
  "aggs": {
    "grades_stats": {
      "extended_stats": {
        "field": "grade",
        "sigma": 3          <1>
      }
    }
  }
}
```
% TEST[setup:exams]

1. `sigma` controls how many standard deviations +/- from the mean should be displayed


`sigma` can be any non-negative double, meaning you can request non-integer values such as `1.5`. A value of `0` is valid, but will simply return the average for both `upper` and `lower` bounds.

The `upper` and `lower` bounds are calculated as population metrics so they are always the same as `upper_population` and `lower_population` respectively.

::::{admonition} Standard Deviation and Bounds require normality
:class: note

The standard deviation and its bounds are displayed by default, but they are not always applicable to all data-sets. Your data must be normally distributed for the metrics to make sense. The statistics behind standard deviations assumes normally distributed data, so if your data is skewed heavily left or right, the value returned will be misleading.

::::



## Script [_script_5]

If you need to aggregate on a value that isn’t indexed, use a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md). Say the we found out that the grades we’ve been working on were for an exam that was above the level of the students and we want to "correct" it:

```console
GET /exams/_search
{
  "size": 0,
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
    "grades_stats": {
      "extended_stats": { "field": "grade.corrected" }
    }
  }
}
```
% TEST[setup:exams]
% TEST[s/_search/_search\?filter_path=aggregations/]


## Missing value [_missing_value_9]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
GET /exams/_search
{
  "size": 0,
  "aggs": {
    "grades_stats": {
      "extended_stats": {
        "field": "grade",
        "missing": 0        <1>
      }
    }
  }
}
```
% TEST[setup:exams]

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `0`.



