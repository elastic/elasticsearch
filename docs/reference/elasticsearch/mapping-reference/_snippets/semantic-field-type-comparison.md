## Should I use [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) or [`semantic`](/reference/elasticsearch/mapping-reference/semantic-field.md)? [should-i-use-semantictext-or-semantic]

Elasticsearch provides two field types that generate and store embeddings automatically. Choose the field type based on the content and embedding model you want to use.

`semantic_text` accepts text only. To index or search images, audio, video, or PDF files, use `semantic` with a compatible multimodal embedding endpoint.

| Aspect | [`semantic`](/reference/elasticsearch/mapping-reference/semantic-field.md) | [`semantic_text`](/reference/elasticsearch/mapping-reference/semantic-text.md) |
|---|---|---|
| Input | Text, images, audio, video, and PDF files | **Text only** |
| Supported {{infer}} task types | `embedding` | `embedding`, `text_embedding`, and `sparse_embedding` |
| Vector storage | Dense vectors only | Dense or sparse vectors |
| `inference_id` | Required. No default endpoint is provided. | Optional. A default endpoint is used when you don't specify one. |
| `search_inference_id` | Optional. If omitted, `inference_id` is used for search. | Optional. If omitted, `inference_id` is used for search. |
| `chunking_settings` | Supported for text input. Chunking does not apply to non-text input. | Supported for text input. |
| `index_options` | Supports `dense_vector` options. | Supports `dense_vector` and `sparse_vector` options. |
| `meta` | Supported. | Supported. |
| Availability | {applies_to}`stack: preview 9.5` {applies_to}`serverless: preview` | {applies_to}`stack: ga 9.0` {applies_to}`serverless: ga` |
