---
navigation_title: "Span near"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-near-query.html
---

# Span near query [query-dsl-span-near-query]


Matches spans which are near one another. One can specify *slop*, the maximum number of intervening unmatched positions, as well as whether matches are required to be in-order. Here is an example:

```console
GET /_search
{
  "query": {
    "span_near": {
      "clauses": [
        { "span_term": { "field": "value1" } },
        { "span_term": { "field": "value2" } },
        { "span_term": { "field": "value3" } }
      ],
      "slop": 12,
      "in_order": false
    }
  }
}
```

The `clauses` element is a list of one or more other span type queries and the `slop` controls the maximum number of intervening unmatched positions permitted.

