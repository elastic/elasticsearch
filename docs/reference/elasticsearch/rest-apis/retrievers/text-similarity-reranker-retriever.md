---
applies_to:
 stack: all
 serverless: ga
---

# Text similarity re-ranker retriever [text-similarity-reranker-retriever]

The `text_similarity_reranker` retriever uses an NLP model to improve search results by reordering the top-k documents based on their semantic similarity to the query.

::::{tip}
Refer to [*Semantic re-ranking*](docs-content://solutions/search/ranking/semantic-reranking.md) for a high level overview of semantic re-ranking.
::::

## Prerequisites [_prerequisites_15]

To use `text_similarity_reranker`, you can rely on the preconfigured `.rerank-v1-elasticsearch` inference endpoint, which uses the [Elastic Rerank model](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md) and serves as the default if no `inference_id` is provided. This model is optimized for reranking based on text similarity. If you'd like to use a different model, you can set up a custom inference endpoint for the `rerank` task using the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put). The endpoint should be configured with a machine learning model capable of computing text similarity. Refer to [the Elastic NLP model reference](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-model-ref.md#ml-nlp-model-ref-text-similarity) for a list of third-party text similarity models supported by {{es}}.

You have the following options:

* Use the built-in [Elastic Rerank](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md) cross-encoder model via the inference API’s {{es}} service. See [this example](https://www.elastic.co/guide/en/elasticsearch/reference/current/infer-service-elasticsearch.html#inference-example-elastic-reranker) for creating an endpoint using the Elastic Rerank model.
* Use the [Cohere Rerank inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the `rerank` task type.
* Use the [Google Vertex AI inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the `rerank` task type.
* Upload a model to {{es}} with [Eland](eland://reference/machine-learning.md#ml-nlp-pytorch) using the `text_similarity` NLP task type.

    * Then set up an [{{es}} service inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) with the `rerank` task type.
    * Refer to the [example](#text-similarity-reranker-retriever-example-eland) on this page for a step-by-step guide.


::::{important}
Scores from the re-ranking process are normalized using the following formula before returned to the user, to avoid having negative scores.

```text
score = max(score, 0) + min(exp(score), 1)
```

Using the above, any initially negative scores are projected to (0, 1) and positive scores to [1, infinity). To revert back if needed, one can use:

```text
score = score - 1, if score >= 0
score = ln(score), if score < 0
```

::::

## Parameters [text-similarity-reranker-retriever-parameters]

`retriever`
:   (Required, `retriever`)

    The child retriever that generates the initial set of top documents to be re-ranked.


`field`
:   (Required, `string`)

    The document field to be used for text similarity comparisons. This field should contain the text that will be evaluated against the `inferenceText`.


`inference_id`
:   (Optional, `string`)

    Unique identifier of the inference endpoint created using the {{infer}} API. If you don’t specify an inference endpoint, the `inference_id` field defaults to `.rerank-v1-elasticsearch`, a preconfigured endpoint for the elasticsearch `.rerank-v1` model.


`inference_text`
:   (Required, `string`)

    The text snippet used as the basis for similarity comparison.


`rank_window_size`
:   (Optional, `int`)

    The number of top documents to consider in the re-ranking process. Defaults to `10`.


`min_score`
:   (Optional, `float`)

    Sets a minimum threshold score for including documents in the re-ranked results. Documents with similarity scores below this threshold will be excluded. Note that score calculations vary depending on the model used.


`filter`
:   (Optional, [query object or list of query objects](/reference/query-languages/querydsl.md))

    Applies the specified [boolean query filter](/reference/query-languages/query-dsl/query-dsl-bool-query.md) to the child  `retriever`. If the child retriever already specifies any filters, then this top-level filter is applied in conjunction with the filter defined in the child retriever.

`chunk_rescorer` {applies_to}`stack: beta 9.2-9.3, ga 9.4+` {applies_to}`serverless: ga`
:   (Optional, `object`)

    Chunks and scores documents based on configured chunking settings, and only sends the best scoring chunks to the reranking model as input. This helps improve relevance when reranking long documents that would otherwise be truncated by the reranking model's token limit. It can also help to control costs by controlling the amount of tokens used for inference.

    Parameters for `chunk_rescorer`:

    `size`
    :   (Optional, `int`)

    The number of chunks to pass to the reranker. Defaults to `1`.

    `chunking_settings`
    :   (Optional, `object`)

    Settings for chunking text into smaller passages for scoring and reranking. By default, chunking settings are configured to fit within the token window of the model associated with the `inference_id`. Refer to the [Inference API documentation](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put#operation-inference-put-body-application-json-chunking_settings) for valid values for `chunking_settings`.
    :::{warning}
    Chunk rescoring is an expert feature. When used with models that do not naively truncate, it can slightly degrade relevance. The default chunking settings are recommended, as if you explicitly configure chunks larger than the reranker's token limit the results may be truncated. This can degrade relevance significantly.
    :::


## Example: Elastic Rerank [text-similarity-reranker-retriever-example-elastic-rerank]

::::{tip}
Refer to this [Python notebook](https://github.com/elastic/elasticsearch-labs/blob/main/notebooks/search/12-semantic-reranking-elastic-rerank.ipynb) for an end-to-end example using Elastic Rerank.

::::


This example demonstrates how to deploy the [Elastic Rerank](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-rerank.md) model and use it to re-rank search results using the `text_similarity_reranker` retriever.

<!--
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

Follow these steps:

1. Create an inference endpoint for the `rerank` task using the [Create {{infer}} API](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put).

    ```console
    PUT _inference/rerank/my-elastic-rerank
    {
      "service": "elasticsearch",
      "service_settings": {
        "model_id": ".rerank-v1",
        "num_threads": 1,
        "adaptive_allocations": { <1>
          "enabled": true,
          "min_number_of_allocations": 1,
          "max_number_of_allocations": 10
        }
      }
    }
    ```
    % TEST[skip:uses ML]

    1. [Adaptive allocations](docs-content://deploy-manage/autoscaling/trained-model-autoscaling.md#enabling-autoscaling-through-apis-adaptive-allocations) will be enabled with the minimum of 1 and the maximum of 10 allocations.

2. Define a `text_similarity_rerank` retriever:

    ```console
    POST _search
    {
      "retriever": {
        "text_similarity_reranker": {
          "retriever": {
            "standard": {
              "query": {
                "match": {
                  "text": "How often does the moon hide the sun?"
                }
              }
            }
          },
          "field": "text",
          "inference_id": "my-elastic-rerank",
          "inference_text": "How often does the moon hide the sun?",
          "rank_window_size": 100,
          "min_score": 0.5
        }
      }
    }
    ```
    % TEST[skip:uses ML]



## Example: Cohere Rerank [text-similarity-reranker-retriever-example-cohere]

This example enables out-of-the-box semantic search by re-ranking top documents using the Cohere Rerank API. This approach eliminates the need to generate and store embeddings for all indexed documents. This requires a [Cohere Rerank inference endpoint](https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-inference-put) that is set up for the `rerank` task type.

```console
GET /index/_search
{
   "retriever": {
      "text_similarity_reranker": {
         "retriever": {
            "standard": {
               "query": {
                  "match_phrase": {
                     "text": "landmark in Paris"
                  }
               }
            }
         },
         "field": "text",
         "inference_id": "my-cohere-rerank-model",
         "inference_text": "Most famous landmark in Paris",
         "rank_window_size": 100,
         "min_score": 0.5
      }
   }
}
```
% TEST[skip:uses ML]


## Example: Semantic re-ranking with a Hugging Face model [text-similarity-reranker-retriever-example-eland]

The following example uses the `cross-encoder/ms-marco-MiniLM-L-6-v2` model from Hugging Face to rerank search results based on semantic similarity. The model must be uploaded to {{es}} using [Eland](eland://reference/machine-learning.md#ml-nlp-pytorch).

::::{tip}
Refer to [the Elastic NLP model reference](docs-content://explore-analyze/machine-learning/nlp/ml-nlp-model-ref.md#ml-nlp-model-ref-text-similarity) for a list of third party text similarity models supported by {{es}}.

::::


Follow these steps to load the model and create a semantic re-ranker.

1. Install Eland using `pip`

    ```sh
    python -m pip install eland[pytorch]
    ```
    % NOTCONSOLE

2. Upload the model to {{es}} using Eland. This example assumes you have an Elastic Cloud deployment and an API key. Refer to the [Eland documentation](eland://reference/machine-learning.md#ml-nlp-pytorch-auth) for more authentication options.

    ```sh
    eland_import_hub_model \
      --cloud-id $CLOUD_ID \
      --es-api-key $ES_API_KEY \
      --hub-model-id cross-encoder/ms-marco-MiniLM-L-6-v2 \
      --task-type text_similarity \
      --clear-previous \
      --start
    ```
    % NOTCONSOLE

3. Create an inference endpoint for the `rerank` task

    ```console
    PUT _inference/rerank/my-msmarco-minilm-model
    {
      "service": "elasticsearch",
      "service_settings": {
        "num_allocations": 1,
        "num_threads": 1,
        "model_id": "cross-encoder__ms-marco-minilm-l-6-v2"
      }
    }
    ```
    % TEST[skip:uses ELSER]

4. Define a `text_similarity_rerank` retriever.

    ```console
    POST movies/_search
    {
      "retriever": {
        "text_similarity_reranker": {
          "retriever": {
            "standard": {
              "query": {
                "match": {
                  "genre": "drama"
                }
              }
            }
          },
          "field": "plot",
          "inference_id": "my-msmarco-minilm-model",
          "inference_text": "films that explore psychological depths"
        }
      }
    }
    ```
    % TEST[skip:uses ELSER]

    This retriever uses a standard `match` query to search the `movie` index for films tagged with the genre "drama". It then re-ranks the results based on semantic similarity to the text in the `inference_text` parameter, using the model we uploaded to {{es}}.
