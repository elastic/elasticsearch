---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-tier-field.html
---

# _tier field [mapping-tier-field]

When performing queries across multiple indexes, it is sometimes desirable to target indexes held on nodes of a given data tier (`data_hot`, `data_warm`, `data_cold` or `data_frozen`). The `_tier` field allows matching on the `tier_preference` setting of the index a document was indexed into. The preferred value is accessible in certain queries :

```console
PUT index_1/_doc/1
{
  "text": "Document in index 1"
}

PUT index_2/_doc/2?refresh=true
{
  "text": "Document in index 2"
}

GET index_1,index_2/_search
{
  "query": {
    "terms": {
      "_tier": ["data_hot", "data_warm"] <1>
    }
  }
}
```

1. Querying on the `_tier` field


Typically a query will use a `terms` query to list the tiers of interest but you can use the `_tier` field in any query that is rewritten to a `term` query, such as the `match`,  `query_string`, `term`, `terms`, or `simple_query_string` query, as well as `prefix` and `wildcard` queries. However, it does not support `regexp` and `fuzzy` queries.

The `tier_preference` setting of the index is a comma-delimited list of tier names in order of preference i.e. the preferred tier for hosting an index is listed first followed by potentially many fall-back options. Query matching only considers the first preference (the first value of a list).

