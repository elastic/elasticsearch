---
applies_to:
  stack:
  serverless:
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html
---

# norms [norms]

Norms store various normalization factors that are later used at query time in order to compute the score of a document relatively to a query.

Although useful for scoring, norms also require quite a lot of disk (typically in the order of one byte per document per field in your index, even for documents that don’t have this specific field). As a consequence, if you don’t need scoring on a specific field, you should disable norms on that field. In particular, this is the case for fields that are used solely for filtering or aggregations.

You can't change the `norms` setting of an existing field. The update mapping API rejects the request.

To turn off norms, set `norms` to `false` when you create the index:

```console
PUT my-index-000001
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "norms": false
      }
    }
  }
}
```

To turn off norms on a field that already has data, use one of these options:

- Update the index template, then [roll over](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-rollover) the data stream. Only new backing indices get the change; existing data keeps its norms.
- [Reindex](docs-content://manage-data/data-store/data-streams/modify-data-stream.md#data-streams-use-reindex-to-change-mappings-settings) into a new index or data stream that has `norms` set to `false`.
