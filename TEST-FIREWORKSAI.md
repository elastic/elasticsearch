# FireworksAI Integration Testing Guide

This guide helps you manually test the FireworksAI embeddings and reranking integration.

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
- ✓ Create rerank endpoint
- ✓ Test document reranking
- ✓ Test top-N limiting
- ✓ Verify all configurations
- ✓ Clean up endpoints

### Option 2: Manual Testing

#### Test Embeddings

```bash
# Create endpoint
curl -X PUT "localhost:9200/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "service": "fireworksai",
  "service_settings": {
    "api_key": "YOUR_API_KEY",
    "model_id": "nomic-ai/nomic-embed-text-v1.5"
  }
}'

# Generate embeddings
curl -X POST "localhost:9200/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "input": ["Hello world", "Elasticsearch is great"]
}'
```

#### Test Reranking

```bash
# Create endpoint
curl -X PUT "localhost:9200/_inference/rerank/fireworks-rerank" \
-H "Content-Type: application/json" \
-d '{
  "service": "fireworksai",
  "service_settings": {
    "api_key": "YOUR_API_KEY",
    "model_id": "jinaai/jina-reranker-v2-base-multilingual"
  }
}'

# Rerank documents
curl -X POST "localhost:9200/_inference/rerank/fireworks-rerank" \
-H "Content-Type: application/json" \
-d '{
  "query": "What is the capital of France?",
  "input": [
    "Paris is the capital of France",
    "Berlin is the capital of Germany"
  ]
}'
```

## Supported Models

### Embeddings
- `nomic-ai/nomic-embed-text-v1.5` (768d) ⭐ **Recommended**
- `WhereIsAI/UAE-Large-V1` (1024d)
- `thenlper/gte-large` (1024d)

### Reranking
- `jinaai/jina-reranker-v2-base-multilingual` ⭐ **Recommended**
- `BAAI/bge-reranker-v2-m3`

## Expected Results

### Successful Embeddings Response
```json
{
  "text_embedding": [
    {
      "embedding": [0.123, -0.456, 0.789, ...]
    }
  ]
}
```

### Successful Rerank Response
```json
{
  "rerank": [
    {
      "index": 0,
      "relevance_score": 0.98,
      "text": "Paris is the capital of France"
    }
  ]
}
```

## Troubleshooting

### Common Errors

**401 Unauthorized**
- Check your API key is correct
- Ensure no extra spaces in the key

**404 Model Not Found**
- Verify the model ID is spelled correctly
- Check model is available on FireworksAI

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
curl -X DELETE "localhost:9200/_inference/rerank/fireworks-rerank"
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
        "dims": 768,
        "index": true,
        "similarity": "cosine"
      }
    }
  }
}'

# Index documents with embeddings
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
- **PR Feedback**: See [PR #137130](https://github.com/elastic/elasticsearch/pull/137130)

## Success Checklist

- [ ] API key obtained from FireworksAI
- [ ] Elasticsearch started successfully
- [ ] Embeddings endpoint created
- [ ] Embeddings generated (got array of floats)
- [ ] Rerank endpoint created
- [ ] Documents reranked (sorted by relevance)
- [ ] No errors in Elasticsearch logs
- [ ] Test script runs without failures

