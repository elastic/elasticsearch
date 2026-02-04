---
navigation_title: "Semantic"
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-semantic-query.html
applies_to:
  stack: ga 9.0
  serverless: ga
---

# Semantic query [query-dsl-semantic-query]

::::{note}
We don't recommend this legacy query type for _new_ projects. Use the match query (with [QueryDSL](/reference/query-languages/query-dsl/query-dsl-match-query.md) or [ESQL](/reference/query-languages/esql/functions-operators/search-functions.md#esql-match)) instead. The semantic query remains available to support existing implementations.
::::

The `semantic` query type enables you to perform [semantic search](docs-content://solutions/search/semantic-search.md) on data stored in a [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) field. This query accepts natural-language text and uses the field’s configured inference endpoint to generate a query embedding and match documents.

For an overview of all query options available for `semantic_text` fields, see [Querying `semantic_text` fields](/reference/elasticsearch/mapping-reference/semantic-text-search-retrieval.md#querying-semantic-text-fields).

## Inference endpoint selection

The target field of `semantic` query must be mapped as `semantic_text` and associated with an inference endpoint. At query time, the inference endpoint is chosen as follows:
- If `search_inference_id` is defined, the semantic query uses that endpoint to embed the query.
- If no `search_inference_id` is defined, `inference_id` is used for both indexing and search.
- If no endpoint is specified at mapping, `inference_id` defaults to `.elser-2-elasticsearch`.￼

The underlying vector type (dense or sparse) is determined by the endpoint automatically. No extra query parameters are required.

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
% TEST[skip: Requires inference endpoints]


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
% TEST[skip: Requires inference endpoints]

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
% TEST[skip: Requires inference endpoints]

