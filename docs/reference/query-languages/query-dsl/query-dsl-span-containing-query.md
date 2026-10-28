---
navigation_title: "Span containing"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-containing-query.html
---

# Span containing query [query-dsl-span-containing-query]


Returns matches which enclose another span query. Here is an example:

```console
GET /_search
{
  "query": {
    "span_containing": {
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

The `big` and `little` clauses can be any span type query. Matching spans from `big` that contain matches from `little` are returned.

