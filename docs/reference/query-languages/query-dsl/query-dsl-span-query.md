---
navigation_title: "Span or"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-or-query.html
---

# Span or query [query-dsl-span-or-query]


Matches the union of its span clauses. Here is an example:

```console
GET /_search
{
  "query": {
    "span_or" : {
      "clauses" : [
        { "span_term" : { "field" : "value1" } },
        { "span_term" : { "field" : "value2" } },
        { "span_term" : { "field" : "value3" } }
      ]
    }
  }
}
```

The `clauses` element is a list of one or more other span type queries.

