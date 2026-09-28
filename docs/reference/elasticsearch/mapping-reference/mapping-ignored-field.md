---
applies_to:
  stack:
  serverless:
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-ignored-field.html
---

# _ignored field [mapping-ignored-field]

The `_ignored` field indexes and stores the names of every field in a document that has been ignored when the document was indexed. This can, for example, be the case:

* When the field was malformed and [`ignore_malformed`](/reference/elasticsearch/mapping-reference/ignore-malformed.md) was turned on.
* When a `keyword` field’s value exceeds its optional [`ignore_above`](/reference/elasticsearch/mapping-reference/ignore-above.md) setting.
* When `index.mapping.total_fields.limit` has been reached and `index.mapping.total_fields.ignore_dynamic_beyond_limit` is set to `true`.
* {applies_to}`stack: preview 9.6` {applies_to}`serverless: preview` When a field in a [`columnar`](/reference/elasticsearch/columnar/index.md) index configured with [`doc_values.on_failure: ignore`](/reference/elasticsearch/mapping-reference/doc-values.md#doc-values-on-failure) receives a document that violates its `multi_value: false` or `nullability: false` constraint.

For more index setting details, refer to [](/reference/elasticsearch/index-settings/mapping-limit.md).

This field is searchable with [`term`](/reference/query-languages/query-dsl/query-dsl-term-query.md), [`terms`](/reference/query-languages/query-dsl/query-dsl-terms-query.md) and [`exists`](/reference/query-languages/query-dsl/query-dsl-exists-query.md) queries, and is returned as part of the search hits.

For instance the below query matches all documents that have one or more fields that got ignored:

```console
GET _search
{
  "query": {
    "exists": {
      "field": "_ignored"
    }
  }
}
```

Similarly, the below query finds all documents whose `@timestamp` field was ignored at index time:

```console
GET _search
{
  "query": {
    "term": {
      "_ignored": "@timestamp"
    }
  }
}
```

Since 8.15.0, the `_ignored` field supports aggregations as well. For example, the below query finds all fields that got ignored:

```console
GET _search
{
  "aggs": {
    "ignored_fields": {
      "terms": {
         "field": "_ignored"
      }
    }
  }
}
```

