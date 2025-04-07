---
navigation_title: "Stats"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-metrics-stats-aggregation.html
---

# Stats aggregation [search-aggregations-metrics-stats-aggregation]


A `multi-value` metrics aggregation that computes stats over numeric values extracted from the aggregated documents.

The stats that are returned consist of: `min`, `max`, `sum`, `count` and `avg`.

Assuming the data consists of documents representing exams grades (between 0 and 100) of students

```console
POST /exams/_search?size=0
{
  "aggs": {
    "grades_stats": { "stats": { "field": "grade" } }
  }
}
```

The above aggregation computes the grades statistics over all documents. The aggregation type is `stats` and the `field` setting defines the numeric field of the documents the stats will be computed on. The above will return the following:

```console-result
{
  ...

  "aggregations": {
    "grades_stats": {
      "count": 2,
      "min": 50.0,
      "max": 100.0,
      "avg": 75.0,
      "sum": 150.0
    }
  }
}
```

The name of the aggregation (`grades_stats` above) also serves as the key by which the aggregation result can be retrieved from the returned response.

## Script [_script_12]

If you need to get the `stats` for something more complex than a single field, run the aggregation on a [runtime field](docs-content://manage-data/data-store/mapping/runtime-fields.md).

```console
POST /exams/_search
{
  "size": 0,
  "runtime_mappings": {
    "grade.weighted": {
      "type": "double",
      "script": """
        emit(doc['grade'].value * doc['weight'].value)
      """
    }
  },
  "aggs": {
    "grades_stats": {
      "stats": {
        "field": "grade.weighted"
      }
    }
  }
}
```


## Missing value [_missing_value_15]

The `missing` parameter defines how documents that are missing a value should be treated. By default they will be ignored but it is also possible to treat them as if they had a value.

```console
POST /exams/_search?size=0
{
  "aggs": {
    "grades_stats": {
      "stats": {
        "field": "grade",
        "missing": 0      <1>
      }
    }
  }
}
```

1. Documents without a value in the `grade` field will fall into the same bucket as documents that have the value `0`.



