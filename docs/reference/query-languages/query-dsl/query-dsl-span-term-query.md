---
navigation_title: "Span term"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-term-query.html
---

# Span term query [query-dsl-span-term-query]


Matches spans containing a term. Here is an example:

```console
GET /_search
{
  "query": {
    "span_term" : { "user.id" : "kimchy" }
  }
}
```

A boost can also be associated with the query:

```console
GET /_search
{
  "query": {
    "span_term" : { "user.id" : { "value" : "kimchy", "boost" : 2.0 } }
  }
}
```

Or :

```console
GET /_search
{
  "query": {
    "span_term" : { "user.id" : { "term" : "kimchy", "boost" : 2.0 } }
  }
}
```

