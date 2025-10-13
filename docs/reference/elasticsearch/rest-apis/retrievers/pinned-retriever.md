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
