---
navigation_title: "Semantic"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-semantic-query.html
---

# Semantic query [query-dsl-semantic-query]

The `semantic` query type enables you to perform [semantic search](docs-content://solutions/search/semantic-search.md) on data stored in a [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) field.


## Example request [semantic-query-example]

```console
GET my-index-000001/_search
{
  "query": {
    "semantic": {
      "field": "inference_field",
      "query": "Best surfing places"
    }
  }
}
```


## Top-level parameters for `semantic` [semantic-query-params]

`field`
:   (Required, string) The `semantic_text` field to perform the query on.

`query`
:   (Required, string) The query text to be searched for on the field.

Refer to [this tutorial](docs-content://solutions/search/semantic-search/semantic-search-semantic-text.md) to learn more about semantic search using `semantic_text` and `semantic` query.


## Hybrid search with the `semantic` query [hybrid-search-semantic]

The `semantic` query can be used as a part of a hybrid search where the `semantic` query is combined with lexical queries. For example, the query below finds documents with the `title` field matching "mountain lake", and combines them with results from a semantic search on the field `title_semantic`, that is a `semantic_text` field. The combined documents are then scored, and the top 3 top scored documents are returned.

```console
POST my-index/_search
{
  "size" : 3,
  "query": {
    "bool": {
      "should": [
        {
          "match": {
            "title": {
              "query": "mountain lake",
              "boost": 1
            }
          }
        },
        {
          "semantic": {
            "field": "title_semantic",
            "query": "mountain lake",
            "boost": 2
          }
        }
      ]
    }
  }
}
```

You can also use semantic_text as part of [Reciprocal Rank Fusion](/reference/elasticsearch/rest-apis/reciprocal-rank-fusion.md) to make ranking relevant results easier:

```console
GET my-index/_search
{
  "retriever": {
    "rrf": {
      "retrievers": [
        {
          "standard": {
            "query": {
              "term": {
                "text": "shoes"
              }
            }
          }
        },
        {
          "standard": {
            "query": {
              "semantic": {
                "field": "semantic_field",
                "query": "shoes"
              }
            }
          }
        }
      ],
      "rank_window_size": 50,
      "rank_constant": 20
    }
  }
}
```

