---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/doc-values.html
---

# doc_values [doc-values]

Most fields are [indexed](/reference/elasticsearch/mapping-reference/mapping-index.md) by default, which makes them searchable. The inverted index allows queries to look up the search term in unique sorted list of terms, and from that immediately have access to the list of documents that contain the term.

Sorting, aggregations, and access to field values in scripts requires a different data access pattern. Instead of looking up the term and finding documents, we need to be able to look up the document and find the terms that it has in a field.

The `doc_values` field is an on-disk data structure that is built at document index time and enables efficient data access. It stores the same values as `_source`, but in a columnar format that is more efficient for sorting and aggregation. 

Doc values are supported on most field types, excluding `text` and `annotated_text` fields. See also [Disabling doc values](#_disabling_doc_values).

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

For all fields that support them, `doc_values` are enabled by default. If you're certain you don't need to sort or aggregate on a field, or access its value from a script, you can disable `doc_values` in order to save disk space.

::::{note}
You cannot disable doc values for [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields.

In some field types, such as [`search_as_you_type`](/reference/elasticsearch/mapping-reference/search-as-you-type.md), doc values appear in API responses but can't be configured. Enabling or disabling `doc_values` for these fields might result in an error or have no effect.
::::

In the following example, `doc_values` is disabled on one field:

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






