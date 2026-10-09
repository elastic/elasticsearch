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

Norms can't be changed on an existing field. To disable norms, set `norms: false` when you create the index:

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

::::{note}
To disable norms for data that's already indexed, update the index template and roll over the data stream, or reindex into a new index.
::::


