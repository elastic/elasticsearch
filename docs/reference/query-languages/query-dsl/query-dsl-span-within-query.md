---
navigation_title: "Span within"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-within-query.html
---

# Span within query [query-dsl-span-within-query]


Returns matches which are enclosed inside another span query. Here is an example:

```console
GET /_search
{
  "query": {
    "span_within": {
      "little": {
        "span_term": { "field1": "foo" }
      },
      "big": {
        "span_near": {
          "clauses": [
            { "span_term": { "field1": "bar" } },
            { "span_term": { "field1": "baz" } }
          ],
          "slop": 5,
          "in_order": true
        }
      }
    }
  }
}
```

The `big` and `little` clauses can be any span type query. Matching spans from `little` that are enclosed within `big` are returned.

