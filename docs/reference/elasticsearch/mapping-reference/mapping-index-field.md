---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-index-field.html
---

# _index field [mapping-index-field]

When performing queries across multiple indexes, it is sometimes desirable to add query clauses that are associated with documents of only certain indexes. The `_index` field allows matching on the index a document was indexed into. Its value is accessible in certain queries and aggregations, and when sorting or scripting:

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
      "_index": ["index_1", "index_2"] <1>
    }
  },
  "aggs": {
    "indices": {
      "terms": {
        "field": "_index", <2>
        "size": 10
      }
    }
  },
  "sort": [
    {
      "_index": { <3>
        "order": "asc"
      }
    }
  ],
  "script_fields": {
    "index_name": {
      "script": {
        "lang": "painless",
        "source": "doc['_index']" <4>
      }
    }
  }
}
```

1. Querying on the `_index` field
2. Aggregating on the `_index` field
3. Sorting on the `_index` field
4. Accessing the `_index` field in scripts


The `_index` field is exposed virtually — it is not added to the Lucene index as a real field. This means that you can use the `_index` field in a `term` or `terms` query (or any query that is rewritten to a `term` query, such as the `match`,  `query_string` or `simple_query_string` query), as well as `prefix` and `wildcard` queries. However, it does not support `regexp` and `fuzzy` queries.

Queries on the `_index` field accept index aliases in addition to concrete index names.

::::{note}
When specifying a remote index name such as `cluster_1:index_3`, the query must contain the separator character `:`. For example, a `wildcard` query on `cluster_*:index_3` would match documents from the remote index. However, a query on `cluster*index_1` is only matched against local indices, since no separator is present. This behavior aligns with the usual resolution rules for remote index names.
::::


