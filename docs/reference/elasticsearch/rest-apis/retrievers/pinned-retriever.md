---
applies_to:
 stack: ga 9.1
 serverless: ga
---

# Pinned retriever [pinned-retriever]

A `pinned` retriever returns top documents by always placing specific documents at the top of the results, with the remaining hits provided by a secondary retriever.

This retriever offers similar functionality to the [pinned query](/reference/query-languages/query-dsl/query-dsl-pinned-query.md), but works seamlessly with other retrievers. This is useful for promoting certain documents for particular queries, regardless of their relevance score.

## Parameters [pinned-retriever-parameters]

`ids`
:   (Optional, array of strings)

    A list of document IDs to pin at the top of the results, in the order provided.

`docs`
:   (Optional, array of objects)

    A list of objects specifying documents to pin. Each object must contain at least an `_id` field, and may also specify `_index` if pinning documents across multiple indices.

`retriever`
:   (Optional, retriever object)

    A retriever (for example a `standard` retriever or a specialized retriever such as `rrf` retriever) used to retrieve the remaining documents after the pinned ones.

Either `ids` or `docs` must be specified.

## Example using `docs` [pinned-retriever-example-documents]

<!--
```console
PUT /restaurants
{
  "mappings": {
    "properties": {
      "region": { "type": "keyword" },
      "year": { "type": "keyword" },
      "vector": {
        "type": "dense_vector",
        "dims": 3
      }
    }
  }
}

POST /restaurants/_bulk?refresh
{"index":{}}
{"region": "Austria", "year": "2019", "vector": [10, 22, 77]}
{"index":{}}
{"region": "France", "year": "2019", "vector": [10, 22, 78]}
{"index":{}}
{"region": "Austria", "year": "2020", "vector": [10, 22, 79]}
{"index":{}}
{"region": "France", "year": "2020", "vector": [10, 22, 80]}

PUT /movies

PUT /books
{
  "mappings": {
    "properties": {
      "title": {
        "type": "text",
        "copy_to": "title_semantic"
      },
      "description": {
        "type": "text",
        "copy_to": "description_semantic"
      },
      "title_semantic": {
        "type": "semantic_text"
      },
      "description_semantic": {
        "type": "semantic_text"
      }
    }
  }
}

PUT _query_rules/my-ruleset
{
    "rules": [
        {
            "rule_id": "my-rule1",
            "type": "pinned",
            "criteria": [
                {
                    "type": "exact",
                    "metadata": "query_string",
                    "values": [ "pugs" ]
                }
            ],
            "actions": {
                "ids": [
                    "id1"
                ]
            }
        }
    ]
}
```
% TESTSETUP

```console
DELETE /restaurants
DELETE /movies
DELETE /books
```
% TEARDOWN
-->

```console
GET /restaurants/_search
{
  "retriever": {
    "pinned": {
      "docs": [
        { "_id": "doc1", "_index": "my-index" },
        { "_id": "doc2" }
      ],
      "retriever": {
        "standard": {
          "query": {
            "match": {
              "title": "elasticsearch"
            }
          }
        }
      }
    }
  }
}
```
