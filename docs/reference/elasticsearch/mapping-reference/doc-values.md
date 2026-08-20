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

For all fields that support them, `doc_values` are turned on by default. If you're certain you don't need to sort or aggregate on a field, or access its value from a script, you can turn off `doc_values` in order to save disk space.

::::{note}
You cannot turn off doc values for [`wildcard`](/reference/elasticsearch/mapping-reference/keyword.md#wildcard-field-type) fields, or for fields in a [`columnar`](/reference/elasticsearch/columnar/index.md) index.

In some field types, such as [`search_as_you_type`](/reference/elasticsearch/mapping-reference/search-as-you-type.md), doc values appear in API responses but can't be configured. Turning `doc_values` on or off for these fields might result in an error or have no effect.
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
2. The `session_id` has `doc_values` turned off, but can still be queried.

## Restricting fields to a single value [doc-values-multi-value]

```{applies_to}
stack: preview 9.5
serverless: preview
```

::::{note}
This setting requires a [`columnar`](/reference/elasticsearch/columnar/index.md) mode index.
::::

By default, all fields allow multiple values per document. In columnar indices, you can restrict a field to at most one value per document by setting `multi_value: false` in the `doc_values` object. If a document is indexed with more than one value for that field, the indexing request is rejected by default. You can change this behavior with the [`on_failure`](#doc-values-on-failure) parameter.

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
stack: preview 9.5
serverless: preview
```

::::{note}
This setting requires a [`columnar`](/reference/elasticsearch/columnar/index.md) mode index.
::::


By default, all fields allow missing or null values. In columnar indices, you can require a field to always carry a value by setting `nullability: false` in the `doc_values` object. If a document is indexed without a value for the field, or with an explicit `null`, the indexing request is rejected by default. You can change this behavior with the [`on_failure`](#doc-values-on-failure) parameter.

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

## Handling doc_values constraint violations [doc-values-on-failure]

```{applies_to}
stack: preview 9.5
serverless: preview
```

::::{note}
This setting requires a [`columnar`](/reference/elasticsearch/columnar/index.md) mode index.
::::

The `on_failure` sub-parameter of `doc_values` controls what happens when a document violates a `multi_value: false` or `nullability: false` constraint. Valid values are `fail` (default) and `ignore`.

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "status_code": { <1>
        "type": "keyword",
        "doc_values": {
          "multi_value": false,
          "on_failure": "fail"
        }
      },
      "tags": { <2>
        "type": "keyword",
        "doc_values": {
          "multi_value": false,
          "on_failure": "ignore"
        }
      }
    }
  }
}
```

1. The `status_code` field rejects any document that contains more than one value.
2. The `tags` field accepts documents with multiple values; only the first value is stored as a queryable doc value, and the rest are redirected to a hidden sidecar.

**`on_failure: fail`** (default)

The whole indexing request for that document is rejected with an error:

- For a `multi_value: false` violation: `Field [x] is configured with [multi_value=false] but encountered multiple values in the same document`
- For a `nullability: false` violation: `Field(s) [x] are configured with [nullability=false] but no value was provided`

**`on_failure: ignore`**

The document is accepted. The behavior depends on which constraint was violated:

- **`multi_value: false`**: The first value is stored in the field's normal doc values. Each additional value is redirected to a hidden per-field `<field>._on_failure` sidecar column. The field name is recorded in [`_ignored`](/reference/elasticsearch/mapping-reference/mapping-ignored-field.md).
- **`nullability: false`**: The missing field is recorded in `_ignored`. There is no value to redirect.

::::{warning}
Redirected values are visible in [`_source`](/reference/elasticsearch/mapping-reference/mapping-source-field.md) only. The sidecar is **not** searchable, not returned by the `fields` API, and not visible to aggregations or ES|QL — the field continues to present itself as single-valued to all of those paths. Only the first value per document participates in search, aggregation, and ES|QL queries.
::::

The following example demonstrates the round-trip. Indexing `["val1","val2","val3"]` into a field mapped with `multi_value: false, on_failure: ignore` keeps `val1` as the queryable doc value and redirects `val2` and `val3` to the sidecar. The `_source` reconstruction returns all three values in their original order; `_ignored` records the field name; and a `term` query on the redirected values finds no documents:

```console
PUT my-on-failure-index
{
  "settings": { "mode": "columnar" },
  "mappings": {
    "properties": {
      "kw": {
        "type": "keyword",
        "doc_values": { "multi_value": false, "on_failure": "ignore" }
      }
    }
  }
}

PUT my-on-failure-index/_doc/1
{ "kw": ["val1", "val2", "val3"] }

GET my-on-failure-index/_doc/1
```
% TEST[skip:requires the doc_values_on_failure feature flag]

The response returns `_source.kw: ["val1","val2","val3"]` and `_ignored: ["kw"]`. A `term` query on `val2` or `val3` returns zero hits.

The index-level setting `index.mapping.doc_values.on_failure` controls the default for all fields in the index. It defaults to `fail`. A field-level value overrides the index setting. Neither can be changed after index creation. Refer to [Doc values settings](/reference/elasticsearch/index-settings/doc-values.md) for a full description of the `index.mapping.doc_values.*` index settings.

## Multi-valued doc values ordering

{{es}} supports storing multi-valued fields at index time. Multi-valued fields can be provided as a JSON array. However in the doc values format, the values aren't stored in the order as was provided at index time. Additionally, duplicates and null values might be lost.
This implementation detail of doc values is visible when features directly interact with doc values, which may be the case for example in ES|QL or aggregations in the search API. Note, that _source always returns arrays in the way that was provided at index time.

How the ordering differs depends on whether the array is mapped as keyword or a numeric field type. In case of the `keyword` field type, the multi-valued values for each document are ordered lexicographically and duplicates are lost. If retaining duplicates is important then the `counted_keyword` field type should be used.
In case of numeric field types (e.g. `long`, `double`, `scaled_float`, etc.), the multi-valued values for each document are ordered in natural order and duplicates are retained.

## Doc values skippers [doc-values-skippers]
```{applies_to}
stack: ga 9.3
```

Doc values skippers are an additional data structure on doc values fields that store summary information for multi-level blocks of documents (currently minimum value, maximum value and doc count).
They can assist fast querying and aggregation over a field without the need for a terms or points index structure, significantly reducing its disk footprint. This is particularly true when the field in question is correlated with the index sort.  For example, timestamp filtered queries in time series indexes can use skippers to filter out large blocks of documents without having to inspect individual field values.

Skippers can be enabled for all fields in an index that are marked as `doc_values: true` and `index: false` by using the index-level setting `index.mapping.use_doc_values_skippers`.  They are enabled by default for [`time_series`](docs-content://manage-data/data-store/data-streams/time-series-data-stream-tsds.md#time-series-mode) and [`columnar`](/reference/elasticsearch/columnar/index.md) index modes.




