---
navigation_title: "Span multi-term"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-span-multi-term-query.html
---

# Span multi-term query [query-dsl-span-multi-term-query]


The `span_multi` query allows you to wrap a `multi term query` (one of wildcard, fuzzy, prefix, range or regexp query) as a `span query`, so it can be nested. Example:

```console
GET /_search
{
  "query": {
    "span_multi": {
      "match": {
        "prefix": { "user.id": { "value": "ki" } }
      }
    }
  }
}
```

A boost can also be associated with the query:

```console
GET /_search
{
  "query": {
    "span_multi": {
      "match": {
        "prefix": { "user.id": { "value": "ki", "boost": 1.08 } }
      }
    }
  }
}
```

::::{warning}
`span_multi` queries will hit too many clauses failure if the number of terms that match the query exceeds the `indices.query.bool.max_clause_count` [search setting](/reference/elasticsearch/configuration-reference/search-settings.md). To avoid an unbounded expansion you can set the [rewrite method](/reference/query-languages/query-dsl/query-dsl-multi-term-rewrite.md) of the multi term query to `top_terms_*` rewrite. Or, if you use `span_multi` on `prefix` query only, you can activate the [`index_prefixes`](/reference/elasticsearch/mapping-reference/index-prefixes.md) field option of the `text` field instead. This will rewrite any prefix query on the field to a single term query that matches the indexed prefix.
::::


