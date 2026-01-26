---
navigation_title: "Match all"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-all-query.html
---

# Match all query [query-dsl-match-all-query]


The most simple query, which matches all documents, giving them all a `_score` of `1.0`.

```console
GET /_search
{
    "query": {
        "match_all": {}
    }
}
```

The `_score` can be changed with the `boost` parameter:

```console
GET /_search
{
  "query": {
    "match_all": { "boost" : 1.2 }
  }
}
```


## Match None Query [query-dsl-match-none-query]

This is the inverse of the `match_all` query, which matches no documents.

```console
GET /_search
{
  "query": {
    "match_none": {}
  }
}
```

