---
applies_to:
  stack: all
  serverless:
---

# Diversify retriever [diversify-retriever]

The diversify retriever is able to pare down results from another retriever to
apply diversification to the top-N results.
This is particularly useful in cases where you need to have relevant, but
non-similar results returned from your query. An example of this may be to
provide more diverse context to a RAG prompt.

Using MMR (Maximum Marginal Relevance) diversification, the retriever discards
any inner retriever results that are too similar to each other based on
the `field` parameter and in reference to any `query_vector` that is provided.
Note that the order of the results from the inner retriever is not changed.

## Parameters [diversify-retriever-parameters]

`type`
:   (Required, string)

    The type of diversification to use. Currently only `mmr` (maximum marginal relevance) is supported.

`field`
:   (Required, string)

    The name of the field that will use its values for the diversification process.
    The field must be a `dense_vector` type.

`num_candidates`
:   (Required, integer)

    The maximum number of top-N results to return.

`retriever`
:   (Required, retriever object)

    A single child retriever to specify which sets of returned top documents will have the diversification applied to them.
    Note that although some of the inner retriever's results may be removed, the rank and order will not change.

`query_vector`
:   (Optional, array of `float` or `byte`)

    Query vector. Must have the same number of dimensions as the vector field you are searching against.
    Must be either an array of floats or a hex-encoded byte vector.

`lambda`
:   (Required if `mmr` is used, float)

    A number between 0.0 and 1.0 specifying how much weight for diversification should be given to the query vector as opposed to the amount of weight given to the field values.

## Example

The following example uses a MMR diversification retriever to diversify and
return the top three results from the inner standard retriever.
The lambda is set at 0.7 which favors the weight from the comparisons of the
vectors in `my_dense_field_vector` over the query vector for determining the
differencs between the documents.

```console
GET my_index/_search
{
  "retriever": {
    "diversify": {
      "type": "mmr",
      "field": "my_dense_vector_field",
      "lambda": 0.7,
      "num_candidates": 3
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
