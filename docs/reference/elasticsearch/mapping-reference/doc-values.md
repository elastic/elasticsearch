---
applies_to:
  stack:
  serverless:
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

## Restricting fields to a single value [doc-values-multi-value]

```{applies_to}
stack: preview
serverless: preview
```

By default, all fields allow multiple values per document. You can restrict a field to at most one value per document by setting `multi_value: false` in the `doc_values` object. If a document is indexed with more than one value for that field, the indexing request is rejected.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": {
        "type": "long",
        "doc_values": {
          "multi_value": false
        }
      }
    }
  }
}
```

The index-level setting `index.mapping.doc_values.multi_value` controls the default for all fields in the index. It defaults to `true` (multiple values allowed).

## Requiring a field to have a value [doc-values-nullability]

```{applies_to}
stack: preview
serverless: preview
```

By default, all fields allow missing or null values. You can require a field to always carry a value by setting `nullability: false` in the `doc_values` object. If a document is indexed without a value for the field, or with an explicit `null`, the indexing request is rejected.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": {
        "type": "long",
        "doc_values": {
          "nullability": false
        }
      }
    }
  }
}
```

If `null_value` is also defined on the field, it serves as a sentinel value for explicit `null` inputs. In that case, `nullability: false` only rejects documents where the field is entirely absent — an explicit `null` is substituted by the sentinel value and accepted.

The index-level setting `index.mapping.doc_values.nullability` will control the default for all fields in the index. It will default to `true` (null values allowed).

## Multi-valued doc values note

Elasticsearch supports storing multi-valued fields at index time. Multi-valued fields can be provided as a json array. However in the doc values format, the values aren't stored in the order as was provided at index time. Additionally, duplicates may be lost.
This implementation detail of doc values is visible when features directly interact with doc values, which may be the case for example in ES|QL or aggregations in the search API. Note, that _source always returns arrays in the way that was provided at index time.

How the ordering differs depends on whether the array is mapped as keyword or a numeric field type. In case of the `keyword` field type, the multi-valued values for each document are ordered lexicographically and duplicates are lost. If retaining duplicates is important then the `counted_keyword` field type should be used.
In case of numeric field types (e.g. `long`, `double`, `scaled_float`, etc.), the multi-valued values for each document are ordered in natural order and duplicates are retained.

## Doc values skippers [doc-values-skippers]
```{applies_to}
stack: ga 9.3
```

Doc values skippers are an additional data structure on doc values fields that store summary information for multi-level blocks of documents (currently minimum value, maximum value and doc count).
They can assist fast querying and aggregation over a field without the need for a terms or points index structure, significantly reducing its disk footprint. This is particularly true when the field in question is correlated with the index sort.  For example, timestamp filtered queries in time series indexes can use skippers to filter out large blocks of documents without having to inspect individual field values.

Skippers can be enabled for all fields in an index that are marked as `docvalues=true` and `index=false` by using the index-level setting `index.mapping.use_doc_values_skippers`.  They are enabled by default for [`time_series indexes`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) and the columnar index modes.




