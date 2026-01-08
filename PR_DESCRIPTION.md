## Summary

Adds FireworksAI integration to the Elasticsearch inference plugin, supporting text embeddings.

This implementation follows the pattern established by other inference services (OpenAI, Cohere, VoyageAI, etc.) and aligns with the structure of PR #134933 (ContextualAI).

## Implementation

**Text Embeddings:**
- OpenAI-compatible API format (`/v1/embeddings`)
- Variable-length embeddings via `dimensions` parameter
- Chunking support for large documents
- Multiple model support (Qwen3 family, Nomic, BERT-based, LLM-based)

## API Reference

Based on [Fireworks AI Embeddings Documentation](https://docs.fireworks.ai/guides/querying-embeddings-models)

---

## Files (17 total)

### Main Implementation (13 files)
```
x-pack/plugin/inference/src/main/java/.../fireworksai/
├── FireworksAiService.java                        # Main service class
├── FireworksAiModel.java                          # Base model class
├── FireworksAiRateLimitServiceSettings.java       # Rate limiting interface
├── FireworksAiResponseHandler.java                # HTTP response handler
├── action/
│   ├── FireworksAiActionCreator.java
│   └── FireworksAiActionVisitor.java
├── embeddings/
│   ├── FireworksAiEmbeddingsModel.java
│   ├── FireworksAiEmbeddingsServiceSettings.java
│   └── FireworksAiEmbeddingsTaskSettings.java
├── request/
│   ├── FireworksAiEmbeddingsRequest.java
│   └── FireworksAiEmbeddingsRequestEntity.java
└── response/
    ├── FireworksAiEmbeddingsResponseEntity.java
    └── FireworksAiErrorResponseEntity.java
```

### Tests (4 files)
- `FireworksAiServiceTests.java`
- `FireworksAiResponseHandlerTests.java`
- `FireworksAiEmbeddingsTaskSettingsTests.java`
- `FireworksAiErrorResponseEntityTests.java`

---

## Usage Example

```bash
# Create an embeddings inference endpoint
PUT _inference/text_embedding/my-fireworks-embeddings
{
  "service": "fireworksai",
  "service_settings": {
    "api_key": "your-api-key",
    "model_id": "fireworks/qwen3-embedding-8b"
  }
}

# Generate embeddings
POST _inference/text_embedding/my-fireworks-embeddings
{
  "input": ["Hello world", "Machine learning is fun"]
}

# With custom dimensions (variable-length embeddings)
POST _inference/text_embedding/my-fireworks-embeddings
{
  "input": ["Hello world"],
  "task_settings": {
    "dimensions": 512
  }
}
```

---

## Configuration Options

### Service Settings
| Setting | Required | Description |
|---------|----------|-------------|
| `api_key` | Yes | FireworksAI API key |
| `model_id` | Yes | Model identifier (e.g., `fireworks/qwen3-embedding-8b`) |
| `url` | No | Custom API endpoint |
| `dimensions` | No | Output embedding dimensions |
| `similarity` | No | Similarity measure (cosine, dot_product, l2_norm) |
| `max_input_tokens` | No | Maximum input tokens |
| `rate_limit` | No | Requests per minute (default: 6000) |

### Task Settings
| Setting | Description |
|---------|-------------|
| `dimensions` | Override dimensions at inference time |

---

## Supported Models

- **Qwen3 Embeddings:** `fireworks/qwen3-embedding-8b`, `4b`, `0p6b`
- **Nomic:** `nomic-ai/nomic-embed-text-v1.5`
- **BERT-based:** `thenlper/gte-large`, `BAAI/bge-base-en-v1.5`, etc.
- **LLM-based:** Any LLM in the Fireworks model library

---

## Checklist

- [x] Follows existing service patterns (similar to ContextualAI PR #134933)
- [x] All precommit checks pass
- [x] Unit tests included
- [x] Transport version registered
- [x] Plugin registration updated
- [x] Integration test updated (`InferenceGetServicesIT`)

---

## Notes

- Simplified to embeddings-only based on reviewer feedback
- Rerank support was removed to focus on core embeddings functionality
- Rate limit default of 6000 RPM per FireworksAI documentation
