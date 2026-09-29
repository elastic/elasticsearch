---
applies_to:
  stack:
  serverless:
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
---

# ignore_above [ignore-above]

Strings longer than the `ignore_above` setting will not be indexed or stored. For arrays of strings, `ignore_above` will be applied for each array element separately and string elements longer than `ignore_above` will not be indexed or stored.

::::{note}
All strings/array elements will still be present in the `_source` field, if the latter is enabled which is the default in Elasticsearch.
::::


::::{note}
{applies_to}`stack: preview 9.6` {applies_to}`serverless: preview` In [columnar index modes](/reference/elasticsearch/columnar/index.md), `ignore_above` has no effect. Columnar `_source` is reconstructed from doc values, so a dropped value would be lost entirely; every value is stored regardless of length, whether or not the field is indexed. This covers the field-level parameter, the `index.mapping.ignore_above` setting, and index-mode defaults. The parameter is still accepted and echoed back by `GET _mapping`, over-limit values remain searchable and retrievable, and the field is not added to `_ignored`. Because nothing is dropped, `ignore_above` no longer shields you from Lucene's term byte-length limit: a value longer than `32766` bytes fails the indexing request. Columnar indices created before 9.6 keep the previous behavior.
::::


```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "message": {
        "type": "keyword",
        "ignore_above": 20 <1>
      }
    }
  }
}

PUT my-index-000001/_doc/1 <2>
{
  "message": "Syntax error"
}

PUT my-index-000001/_doc/2 <3>
{
  "message": "Syntax error with some long stacktrace"
}

GET my-index-000001/_search <4>
{
  "aggs": {
    "messages": {
      "terms": {
        "field": "message"
      }
    }
  }
}
```

1. This field will ignore any string longer than 20 characters.
2. This document is indexed successfully.
3. This document will be indexed, but without indexing the `message` field.
4. Search returns both documents, but only the first is present in the terms aggregation.


::::{tip}
The `ignore_above` setting can be updated on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


Outside columnar index modes, this option is also useful for protecting against Lucene’s term byte-length limit of `32766`.

::::{note}
The value for `ignore_above` is the *character count*, but Lucene counts bytes. If you use UTF-8 text with many non-ASCII characters, you may want to set the limit to `32766 / 4 = 8191` since UTF-8 characters may occupy at most 4 bytes.
::::


