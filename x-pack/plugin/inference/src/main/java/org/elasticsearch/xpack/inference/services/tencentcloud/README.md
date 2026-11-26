# TencentCloud multimodal embeddings

The `tencentcloud` service supports the `embedding` task for text, images, and
videos through the TencentCloud AI Gateway `/v1/multimodal-embeddings` API.
The existing `text_embedding` task continues to use `/v1/embeddings`.

## Create an endpoint

```http
PUT _inference/embedding/tencentcloud-multimodal
{
  "service": "tencentcloud",
  "service_settings": {
    "model_id": "EmbeddingGemma-2",
    "region": "bj",
    "api_key": "<API_KEY>"
  }
}
```

The region defaults to `bj`. The service discovers the output dimensions when
creating the endpoint and defaults to cosine similarity. The `dimensions`
setting cannot be supplied in a create request because the provider does not
accept an output-dimension override.

## Generate embeddings

```http
POST _inference/embedding/tencentcloud-multimodal?timeout=300s
{
  "input": [
    "A cat running across a field",
    {
      "content": {
        "type": "image",
        "format": "url",
        "value": "https://example.com/cat.png"
      }
    },
    {
      "content": {
        "type": "video",
        "format": "url",
        "value": "https://example.com/cat.mp4"
      }
    }
  ],
  "input_type": "search"
}
```

Replace the example URLs with media URLs accessible to your TencentCloud AI
Gateway deployment. Video requests may require a longer timeout, as shown above.
Each input produces one embedding, returned in the same order as the inputs.
Each `content` object must contain one item; combining several items into one
embedding is unsupported.

| Input | Formats |
| --- | --- |
| Text | Plain strings or `type: text` |
| Image | URL, or a base64 data URI with a PNG, JPEG, GIF, or WebP MIME type |
| Video | URL, or a base64 data URI with the `video/mp4` MIME type |

For non-text inputs, the format defaults to `base64`. Base64 images require a
prefix such as `data:image/png;base64,`; videos require `data:video/mp4;base64,`.
Audio and PDF inputs are rejected before sending a provider request.

## Batching

Requests are split automatically using these limits per provider call:

| Model | Texts | Images | Videos |
| --- | ---: | ---: | ---: |
| WeMM-Embedding-2B | 120 | 8 | 4 |
| WeMM-Embedding-9B | 60 | 8 | 4 |
| EmbeddingGemma-2 | 200 | 8 | 4 |
| Other models | 60 | 8 | 4 |

A batch cannot mix URL and base64 inputs within the same modality. Calls use the
existing sender's rate limiting and retries. If a batch fails or the provider
returns missing, duplicate, empty, or unexpected embeddings, the request fails.
The same limits apply to chunked inference requests, where each chunk batch is
split further before it is sent.

`max_input_tokens` truncates each text input to that many tokens, using the same
character-based estimate as other embedding services. Images and videos are sent
unchanged.

## Text task prefixes

For `EmbeddingGemma-2`, `input_type` selects a text prefix:

| Input type | Provider `prompt_name` |
| --- | --- |
| `search` | `SearchQuery` |
| `ingest` | `Document` |
| `clustering` | `Clustering` |
| `classification` | `Classification` |

Set `task_settings.input_type` to provide an endpoint default. An explicit
`task_settings.prompt_name` overrides the request's `input_type`, which overrides
the endpoint's `task_settings.input_type`. Request task settings override the
corresponding endpoint task settings.

Accepted, case-sensitive prompt names are `SearchQuery`, `Document`, `document`,
`query`, `Retrieval`, `Retrieval-query`, `Retrieval-document`, `CodeRetrieval`,
`Clustering`, `Classification`, `STS`, `SentenceSimilarity`, `QuestionAnswering`,
`BitextMining`, `Reranking`, `FactChecking`, `InstructionRetrieval`,
`MultilabelClassification`, `PairClassification`, and `Summarization`.
`Reranking` is a text embedding prefix; it does not change the task to `rerank`.
The prefix is omitted for other models and for batches without text.

## Error handling

HTTP 429 responses are retried. HTTP 500 responses with provider error codes
`InvalidParameterValue` or `RequestLimitExceeded` are not retried because they
indicate an invalid request or a response exceeding the provider's size limit.
Other HTTP 500 responses and HTTP 502 responses are retried. Other client errors
are returned without retrying.
