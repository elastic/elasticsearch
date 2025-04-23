---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/painless/current/painless-runtime-fields.html
---

# Use painless scripts in runtime fields [painless-runtime-fields]

A runtime field is a field that is evaluated at query time. When you define a runtime field, you can immediately use it in search requests, aggregations, filtering, and sorting.

When defining a runtime field, you can include a Painless script that is evaluated at query time. This script has access to the entire context of a document, including the original document [`_source` field](docs-content://explore-analyze/scripting/modules-scripting-fields.md) and any mapped fields plus their values. At query time, the script runs and generates values for each scripted field that is included in the query.

You can map a runtime field in the `runtime` section under the mapping definition, or define runtime fields that exist only as part of a search request. The script syntax is the same, regardless of where you define the runtime field.

::::{important}
When defining a Painless script to use with runtime fields, you must include `emit` to return calculated values.
::::



## Define a runtime field in the mapping [painless-runtime-fields-mapping]

Add a `runtime` section under the [mapping definition](docs-content://manage-data/data-store/mapping/map-runtime-field.md) to explore your data without indexing fields.

The script in the following request extracts the day of the week from the `@timestamp` field, which is defined as a `date` type. The script calculates the day of the week based on the value of `@timestamp`, and uses `emit` to return the calculated value.

```console
PUT my-index/
{
  "mappings": {
    "runtime": {
      "day_of_week": {
        "type": "keyword",
        "script": {
          "source":
          """emit(doc['@timestamp'].value.dayOfWeekEnum
          .getDisplayName(TextStyle.FULL, Locale.ROOT))"""
        }
      }
    },
    "properties": {
      "@timestamp": {"type": "date"}
    }
  }
}
```


## Define a runtime field only in a search request [painless-runtime-fields-query]

Use runtime fields in a search request to create a field that exists [only as part of the query](docs-content://manage-data/data-store/mapping/define-runtime-fields-in-search-request.md). You can also [override field values](docs-content://manage-data/data-store/mapping/override-field-values-at-query-time.md) at query time for existing fields without modifying the field itself.

This flexibility allows you to experiment with your data schema and fix mistakes in your index mapping without reindexing your data.

In the following request, the values for the `day_of_week` field are calculated dynamically, and only within the context of this search request:

```console
GET my-index/_search
{
  "runtime_mappings": {
    "day_of_week": {
      "type": "keyword",
      "script": {
        "source":
        """emit(doc['@timestamp'].value.dayOfWeekEnum
        .getDisplayName(TextStyle.FULL, Locale.ROOT))"""
      }
    }
  },
  "aggs": {
    "day_of_week": {
      "terms": {
        "field": "day_of_week"
      }
    }
  }
}
```

