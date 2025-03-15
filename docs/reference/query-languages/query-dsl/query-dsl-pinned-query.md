---
mapped_pages:
  - https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-pinned-query.html
---

# Pinned query [query-dsl-pinned-query]

Promotes selected documents to rank higher than those matching a given query. This feature is typically used to guide searchers to curated documents that are promoted over and above any "organic" matches for a search. The promoted or "pinned" documents are identified using the document IDs stored in the [`_id`](/reference/elasticsearch/mapping-reference/mapping-id-field.md) field.

## Example request [_example_request]

```console
GET /_search
{
  "query": {
    "pinned": {
      "ids": [ "1", "4", "100" ],
      "organic": {
        "match": {
          "description": "iphone"
        }
      }
    }
  }
}
```


## Top-level parameters for `pinned` [pinned-query-top-level-parameters]

`ids`
:   (Optional, array) [Document IDs](/reference/elasticsearch/mapping-reference/mapping-id-field.md) listed in the order they are to appear in results. Required if `docs` is not specified.

`docs`
:   (Optional, array) Documents listed in the order they are to appear in results. Required if `ids` is not specified. You can specify the following attributes for each document:

    `_id`
    :   (Required, string) The unique [document ID](/reference/elasticsearch/mapping-reference/mapping-id-field.md).

    `_index`
    :   (Optional, string) The index that contains the document.


`organic`
:   Any choice of query used to rank documents which will be ranked below the "pinned" documents.


## Pin documents in a specific index [_pin_documents_in_a_specific_index]

If you’re searching over multiple indices, you can pin a document within a specific index using `docs`:

```console
GET /_search
{
  "query": {
    "pinned": {
      "docs": [
        {
          "_index": "my-index-000001", <1>
          "_id": "1"
        },
        {
          "_id": "4" <2>
        }
      ],
      "organic": {
        "match": {
          "description": "iphone"
        }
      }
    }
  }
}
```

1. The document with id `1` from `my-index-000001` will be the first result.
2. When `_index` is missing, all documents with id `4` from the queried indices will be pinned with the same score.



