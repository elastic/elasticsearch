---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html
---

# doc_values [doc-values]

Most fields are [indexed](/reference/elasticsearch/mapping-reference/mapping-index.md) by default, which makes them searchable. The inverted index allows queries to look up the search term in unique sorted list of terms, and from that immediately have access to the list of documents that contain the term.

Sorting, aggregations, and access to field values in scripts requires a different data access pattern. Instead of looking up the term and finding documents, we need to be able to look up the document and find the terms that it has in a field.

Doc values are the on-disk data structure, built at document index time, which makes this data access pattern possible. They store the same values as the `_source` but in a column-oriented fashion that is way more efficient for sorting and aggregations. Doc values are supported on almost all field types, with the *notable exception of `text` and `annotated_text` fields*.

## Doc-value-only fields [doc-value-only-fields]

[Numeric types](/reference/elasticsearch/mapping-reference/number.md), [date types](/reference/elasticsearch/mapping-reference/date.md), the [boolean type](/reference/elasticsearch/mapping-reference/boolean.md), [ip type](/reference/elasticsearch/mapping-reference/ip.md), [geo_point type](/reference/elasticsearch/mapping-reference/geo-point.md) and the [keyword type](/reference/elasticsearch/mapping-reference/keyword.md) can also be queried when they are not [indexed](/reference/elasticsearch/mapping-reference/mapping-index.md) but only have doc values enabled. Query performance on doc values is much slower than on index structures, but offers an interesting tradeoff between disk usage and query performance for fields that are only rarely queried and where query performance is not as important. This makes doc-value-only fields a good fit for fields that are not expected to be normally used for filtering, for example gauges or counters on metric data.

Doc-value-only fields can be configured as follows:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": { <1>
        "type":  "long"
      },
      "session_id": { <2>
        "type":  "long",
        "index": false
      }
    }
  }
}
```

1. The `status_code` field is a regular long field.
2. The `session_id` field has `index` disabled, and is therefore a doc-value-only long field as doc values are enabled by default.



## Disabling doc values [_disabling_doc_values]

All fields which support doc values have them enabled by default. If you are sure that you don’t need to sort or aggregate on a field, or access the field value from a script, you can disable doc values in order to save disk space:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": { <1>
        "type":       "keyword"
      },
      "session_id": { <2>
        "type":       "keyword",
        "doc_values": false
      }
    }
  }
}
```

1. The `status_code` field has `doc_values` enabled by default.
2. The `session_id` has `doc_values` disabled, but can still be queried.


::::{note}
You cannot disable doc values for [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields.
::::



