---
applies_to:
  stack: preview 9.3
  serverless: preview
---

# Diversify retriever [diversify-retriever]

The `diversify` retriever reduces the result set from another retriever by applying a diversification strategy to the top-N results.
This is useful when you want to maximize diversity by preventing similar documents from dominating the top results returned from a search.
Practical use cases include:
- **eCommerce applications**: Show users a wider variety of products rather than multiple similar items
- **Retrieval augmented generation (RAG) workflows**: Provide more diverse context to the LLM, reducing redundancy in the prompt
-
The retriever uses [MMR (Maximum Marginal Relevance)](https://www.cs.cmu.edu/~jgc/publication/The_Use_MMR_Diversity_Based_LTMIR_1998.pdf) diversification to discard results that are too similar to each other.
Similarity is determined based on the `field` parameter and the optionally provided `query_vector`.
:::{note}
The ordering of results returned from the inner retriever is preserved.
:::

## Parameters [diversify-retriever-parameters]

`type`
:   (Required, string)

    The type of diversification to use. Currently only `mmr` (maximum marginal relevance) is supported.

`field`
:   (Required, string)

    The name of the field that will use its values for the diversification process.
    The field must be a `dense_vector` type.

`rank_window_size`
:   (Optional, integer)

    The maximum number of top-documents the `diversify` retriever will receive from the inner retriever.
    Defaults to 10.

`retriever`
:   (Required, retriever object)

    A single child retriever that provides the initial result set for diversification.
    :::{note}
    Although some of the inner retriever's results may be removed, the rank and order of the remaining documents is preserved.
    :::

`query_vector`
:   (Optional, array of `float` or `byte`)

    Query vector. Must have the same number of dimensions as the vector field you are searching against.
    Must be either an array of floats or a hex-encoded byte vector.

`lambda`
:   (Required for `mmr`, float)

    A value between 0.0 and 1.0 that controls how similarity is calculated during diversification. Higher values weight comparisons between field values more heavily, lower values weight the query vector more heavily.

`size`
:   (Optional, only if `mmr` is used, integer)

    The maximum number of top-N results to return. Defaults to 10.

## Example

The following example uses a `diversify` retriever of type `mmr` to diversify and
return the top three results from the inner standard retriever.
The `lambda` value of 0.7 weights `my_dense_field_vector` comparisons more heavily than query vector similarity when determining document differences.

```console
GET my_index/_search
{
  "retriever": {
    "diversify": {
      "type": "mmr",
      "field": "my_dense_vector_field",
      "lambda": 0.7,
      "size": 3
      "query_vector": [0.1, 0.2, 0.3],
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
