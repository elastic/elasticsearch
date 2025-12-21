---
applies_to:
  stack:
  serverless:
navigation_title: "Match Only Text"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/match-only-text.html
---

# Match-only text field type [match-only-text-field-type]

A variant of [`text`](/reference/elasticsearch/mapping-reference/text.md) that trades scoring and efficiency of positional queries for space efficiency. This field effectively stores data the same way as a `text` field that only indexes documents (`index_options: docs`) and disables norms (`norms: false`). Term queries perform as fast if not faster as on `text` fields, however queries that need positions such as the [`match_phrase` query](/reference/query-languages/query-dsl/query-dsl-match-query-phrase.md) perform slower as they need to look at the `_source` document to verify whether a phrase matches. All queries return constant scores that are equal to 1.0.

Analysis is not configurable: text is always analyzed with the [default analyzer](docs-content://manage-data/data-store/text-analysis/specify-an-analyzer.md#specify-index-time-default-analyzer) ([`standard`](/reference/text-analysis/analysis-standard-analyzer.md) by default).

[span queries](/reference/query-languages/query-dsl/span-queries.md) are not supported with this field, use [interval queries](/reference/query-languages/query-dsl/query-dsl-intervals-query.md) instead, or the [`text`](/reference/elasticsearch/mapping-reference/text.md) field type if you absolutely need span queries.

Other than that, `match_only_text` supports the same queries as `text`. And like `text`, it does not support sorting and has only limited support for aggregations.

```console
PUT logs
{
  "mappings": {
    "properties": {
      "@timestamp": {
        "type": "date"
      },
      "message": {
        "type": "match_only_text"
      }
    }
  }
}
```


## Parameters for match-only text fields [match-only-text-params]

The following mapping parameters are accepted:

[`fields`](/reference/elasticsearch/mapping-reference/multi-fields.md)
:   Multi-fields allow the same string value to be indexed in multiple ways for different purposes, such as one field for search and a multi-field for sorting and aggregations, or the same string value analyzed by different analyzers.

[`meta`](/reference/elasticsearch/mapping-reference/mapping-field-meta.md)
:   Metadata about the field.

