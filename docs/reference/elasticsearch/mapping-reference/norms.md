---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/norms.html
---

# norms [norms]

Norms store various normalization factors that are later used at query time in order to compute the score of a document relatively to a query.

Although useful for scoring, norms also require quite a lot of disk (typically in the order of one byte per document per field in your index, even for documents that don’t have this specific field). As a consequence, if you don’t need scoring on a specific field, you should disable norms on that field. In particular, this is the case for fields that are used solely for filtering or aggregations.

::::{tip}
Norms can be disabled on existing fields using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping).
::::


Norms can be disabled (but not reenabled after the fact), using the [update mapping API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping) like so:

```console
PUT my-index-000001/_mapping
{
  "properties": {
    "title": {
      "type": "text",
      "norms": false
    }
  }
}
```

::::{note}
Norms will not be removed instantly, but will be removed as old segments are merged into new segments as you continue indexing new documents. Any score computation on a field that has had norms removed might return inconsistent results since some documents won’t have norms anymore while other documents might still have norms.
::::


