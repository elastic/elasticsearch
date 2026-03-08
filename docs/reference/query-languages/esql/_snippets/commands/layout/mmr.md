```yaml {applies_to}
serverless: preview
stack: 9.4 preview
```

The `MMR` command reduces the result set from another retriever by applying a diversification strategy to the return rows.

## Syntax

```esql
MMR [query_vector] ON field LIMIT limit [WITH { "lambda": lambda_value }]
```

## Parameters

`field`
:   (required) The name of the field that will use its values for the diversification process.
    The field must be a `dense_vector` type.

`limit`
:   (required) The maximum number of rows to return after diversification.

`query_vector`
:   (optional) The query vector to use as part of the diversification algorithm for comparison. 
    Must have the same number of dimensions as the vector field you are searching against.
    Must be one of:
      - An array of floats
      - A hex-encoded byte vector (one byte per dimension; for `bit`, one byte per 8 dimensions){applies_to}`stack: ga 9.0-9.3`
      - A base64-encoded vector string. Base64 supports `float` and `bfloat16` (big-endian), `byte`, and `bit` encodings depending on the target field type. {applies_to}`stack: ga 9.4`
      - A function or expression that returns a `dense_vector`

`lambda_value`
:   (Required if `WITH` is used) A value between 0.0 and 1.0 that controls how similarity is calculated during diversification. 
    Higher values weight the similarity to the query_vector more heavily, lower values weight the diversity more heavily.

## Description

Use the `MMR` command to return a limited, but diverse, set of row results.

This is useful when you want to maximize diversity by preventing similar documents from dominating the top results returned from a search.
Practical use cases include:
- **eCommerce applications**: Show users a wider variety of products rather than multiple similar items
- **Retrieval augmented generation (RAG) workflows**: Provide more diverse context to the LLM, reducing redundancy in the prompt

The command uses [MMR (Maximum Marginal Relevance)](https://www.cs.cmu.edu/~jgc/publication/The_Use_MMR_Diversity_Based_LTMIR_1998.pdf) diversification to discard results that are too similar to each other.
Similarity is determined based on the `field` parameter and the optionally provided `query_vector`.

:::{note}
The ordering of results returned from the inner retriever is preserved.
:::

## Examples

:::{include} ../examples/mmr.csv-spec/simpleMMR.md
:::

:::{include} ../examples/mmr.csv-spec/simpleMMRWithQueryVector.md
:::

:::{include} ../examples/mmr.csv-spec/simpleMMRWithTextEmbedding.md
:::
