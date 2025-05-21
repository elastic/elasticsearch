---
navigation_title: "Span first"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-first-query.html
---

# Span first query [query-dsl-span-first-query]


Matches spans near the beginning of a field. Here is an example:

```console
GET /_search
{
  "query": {
    "span_first": {
      "match": {
        "span_term": { "user.id": "kimchy" }
      },
      "end": 3
    }
  }
}
```

The `match` clause can be any other span type query. The `end` controls the maximum end position permitted in a match.

