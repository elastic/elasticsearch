# FireworksAI Embeddings Integration Testing Guide

This guide helps you manually test the FireworksAI embeddings integration.

## Prerequisites

1. **FireworksAI API Key**
   - Sign up at [fireworks.ai](https://fireworks.ai/)
   - Create an API key from your account settings
   - Copy the key (format: `fw_...`)

2. **Elasticsearch Running Locally**
   - Start with: `./gradlew :run -Dtests.es.xpack.license.self_generated.type=trial`
   - Wait for "started" message (1-2 minutes)
   - Elasticsearch will be available at `http://localhost:9200`

3. **Dependencies**
   - `curl` - for HTTP requests
   - `jq` - for JSON parsing (install: `brew install jq` on macOS)

## Quick Start

### Option 1: Automated Test Script

The easiest way to test everything:

```bash
# Set your API key
export FIREWORKS_API_KEY="fw_your_api_key_here"

# Run the test script
./test-fireworksai.sh
```

The script will:
- ✓ Wait for Elasticsearch to be ready
- ✓ Create embeddings endpoint
- ✓ Test embedding generation
- ✓ Test dimension overrides
- ✓ Verify configurations
- ✓ Clean up endpoints

### Option 2: Manual Testing

#### Test Embeddings

```bash
# Create endpoint with Qwen3 embeddings model (serverless)
curl -X PUT "localhost:9200/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "service": "fireworksai",
  "service_settings": {
    "api_key": "YOUR_API_KEY",
    "model_id": "fireworks/qwen3-embedding-8b"
  }
}'

# Generate embeddings
curl -X POST "localhost:9200/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "input": ["Hello world", "Elasticsearch is great"]
}'

# Generate embeddings with custom dimensions (variable-length)
curl -X POST "localhost:9200/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "input": ["Test with custom dimensions"],
  "task_settings": {
    "dimensions": 512
  }
}'
```

## Supported Models

### Embeddings (Serverless)

Based on [Fireworks AI Embeddings Documentation](https://docs.fireworks.ai/guides/querying-embeddings-models):

| Model | Description |
|-------|-------------|
| `fireworks/qwen3-embedding-8b` | ⭐ **Recommended** - Qwen3 8B embeddings (serverless) |
| `fireworks/qwen3-embedding-4b` | Qwen3 4B embeddings |
| `fireworks/qwen3-embedding-0p6b` | Qwen3 0.6B embeddings |

### Legacy BERT-based Models (Serverless)

| Model | Dimensions |
|-------|------------|
| `nomic-ai/nomic-embed-text-v1.5` | 768 |
| `thenlper/gte-large` | 1024 |
| `BAAI/bge-base-en-v1.5` | 768 |
| `sentence-transformers/all-MiniLM-L6-v2` | 384 |

## Expected Results

### Successful Embeddings Response

```json
{
  "text_embedding": [
    {
      "embedding": [0.123, -0.456, 0.789, ...]
    },
    {
      "embedding": [0.321, -0.654, 0.987, ...]
    }
  ]
}
```

### Successful Endpoint Creation

```json
{
  "inference_id": "fireworks-embeddings",
  "task_type": "text_embedding",
  "service": "fireworksai",
  "service_settings": {
    "model_id": "fireworks/qwen3-embedding-8b",
    "rate_limit": {
      "requests_per_minute": 6000
    }
  }
}
```

## Troubleshooting

### Common Errors

**401 Unauthorized**
- Check your API key is correct
- Ensure no extra spaces in the key

**404 Model Not Found**
- Verify the model ID is spelled correctly
- Check model is available on FireworksAI serverless

**Connection Refused**
- Elasticsearch hasn't started yet - wait longer
- Check Elasticsearch logs: `tail -f build/testclusters/runTask-0/logs/elasticsearch.log`

**429 Rate Limit**
- Wait a few seconds between requests
- Check your FireworksAI plan limits

### Cleanup

Delete test endpoints:
```bash
curl -X DELETE "localhost:9200/_inference/text_embedding/fireworks-embeddings"
```

## Advanced Testing

### Test with Real Data

Create an index and test semantic search:

```bash
# Create index with embeddings
curl -X PUT "localhost:9200/my-index" \
-H "Content-Type: application/json" \
-d '{
  "mappings": {
    "properties": {
      "text": { "type": "text" },
      "text_embedding": {
        "type": "dense_vector",
        "dims": 4096,
        "index": true,
        "similarity": "cosine"
      }
    }
  }
}'

# Index documents with embeddings using inference pipeline
curl -X POST "localhost:9200/_ingest/pipeline/_simulate" \
-H "Content-Type: application/json" \
-d '{
  "pipeline": {
    "processors": [
      {
        "inference": {
          "model_id": "fireworks-embeddings",
          "input_output": {
            "input_field": "text",
            "output_field": "text_embedding"
          }
        }
      }
    ]
  },
  "docs": [
    {
      "_source": {
        "text": "Elasticsearch is a distributed search engine"
      }
    }
  ]
}'
```

## Getting Help

- **Elasticsearch Issues**: Check logs and GitHub issues
- **FireworksAI Issues**: Visit [fireworks.ai/support](https://fireworks.ai/support)
- **API Documentation**: [Fireworks AI Embeddings Guide](https://docs.fireworks.ai/guides/querying-embeddings-models)
- **PR Feedback**: See [PR #137130](https://github.com/elastic/elasticsearch/pull/137130)

## Success Checklist

- [ ] API key obtained from FireworksAI
- [ ] Elasticsearch started successfully
- [ ] Embeddings endpoint created with `fireworks/qwen3-embedding-8b`
- [ ] Embeddings generated (got array of floats)
- [ ] Dimension override works (got embeddings with custom size)
- [ ] No errors in Elasticsearch logs
- [ ] Test script runs without failures
