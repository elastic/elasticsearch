# VoyageAI Manual End-to-End Testing Guide

This guide provides manual test scripts to verify the VoyageAI integration with real API calls.

## Prerequisites

1. VoyageAI API key
2. Running Elasticsearch cluster (local or remote)
3. `curl` or similar HTTP client

## Setup

Export your VoyageAI API key:
```bash
export VOYAGE_API_KEY="your-api-key-here"
export ES_URL="http://localhost:9200"
export ES_USER="elastic"
export ES_PASSWORD="changeme"
```

## Test 1: Text Embeddings (voyage-3 model)

### Create Inference Endpoint
```bash
curl -X PUT "${ES_URL}/_inference/text_embedding/voyage-text-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "voyageai",
    "service_settings": {
      "api_key": "'"${VOYAGE_API_KEY}"'",
      "model_id": "voyage-3"
    }
  }'
```

### Test Inference
```bash
curl -X POST "${ES_URL}/_inference/text_embedding/voyage-text-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["Hello world", "This is a test"]
  }'
```

**Expected**: Returns embeddings with 1024 dimensions (default for voyage-3)

### Cleanup
```bash
curl -X DELETE "${ES_URL}/_inference/text_embedding/voyage-text-test" \
  -u "${ES_USER}:${ES_PASSWORD}"
```

## Test 2: Multimodal Embeddings (voyage-multimodal-3 model)

### Create Inference Endpoint
```bash
curl -X PUT "${ES_URL}/_inference/text_embedding/voyage-multimodal-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "voyageai",
    "service_settings": {
      "api_key": "'"${VOYAGE_API_KEY}"'",
      "model_id": "voyage-multimodal-3"
    }
  }'
```

### Verify Correct Endpoint is Used
Check the model configuration:
```bash
curl -X GET "${ES_URL}/_inference/text_embedding/voyage-multimodal-test" \
  -u "${ES_USER}:${ES_PASSWORD}"
```

**Expected**: Model type should be `VoyageAIMultimodalEmbeddingsModel` (verify in logs if needed)

### Test Inference
```bash
curl -X POST "${ES_URL}/_inference/text_embedding/voyage-multimodal-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["Multimodal test text"]
  }'
```

**Expected**:
- Returns embeddings successfully
- Uses `/v1/multimodal-embeddings` endpoint (check Elasticsearch logs)
- Request JSON uses "inputs" (plural) field

### Cleanup
```bash
curl -X DELETE "${ES_URL}/_inference/text_embedding/voyage-multimodal-test" \
  -u "${ES_USER}:${ES_PASSWORD}"
```

## Test 3: Contextual Embeddings (voyage-context-3 model)

### Create Inference Endpoint
```bash
curl -X PUT "${ES_URL}/_inference/text_embedding/voyage-contextual-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "voyageai",
    "service_settings": {
      "api_key": "'"${VOYAGE_API_KEY}"'",
      "model_id": "voyage-context-3"
    }
  }'
```

### Test Inference
```bash
curl -X POST "${ES_URL}/_inference/text_embedding/voyage-contextual-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["Contextual embeddings test", "Another test sentence"]
  }'
```

**Expected**:
- Returns embeddings successfully
- Uses `/v1/contextualized-embeddings` endpoint (check Elasticsearch logs)
- Request JSON wraps each input in a nested array: `[[" sentence1"], ["sentence2"]]`
- Response has nested "embeddings" (plural) field that gets flattened

### Cleanup
```bash
curl -X DELETE "${ES_URL}/_inference/text_embedding/voyage-contextual-test" \
  -u "${ES_USER}:${ES_PASSWORD}"
```

## Test 4: Input Type Handling

### Test with INGEST input type
```bash
curl -X POST "${ES_URL}/_inference/text_embedding/voyage-text-test?input_type=ingest" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["Test document for ingestion"]
  }'
```

**Expected**: Request includes `"input_type": "document"` in the API call

### Test with SEARCH input type
```bash
curl -X POST "${ES_URL}/_inference/text_embedding/voyage-text-test?input_type=search" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "input": ["Search query"]
  }'
```

**Expected**: Request includes `"input_type": "query"` in the API call

## Test 5: Batch Size Verification

Check that models use correct batch sizes (verify in logs):

- **voyage-3.5**: batch size = 10
- **voyage-3.5-lite**: batch size = 30
- **voyage-context-3**: batch size = 7
- **voyage-multimodal-3**: batch size = 7
- **voyage-3**: batch size = 10

## Test 6: Rerank (Existing Functionality - Should Still Work)

### Create Rerank Endpoint
```bash
curl -X PUT "${ES_URL}/_inference/rerank/voyage-rerank-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "voyageai",
    "service_settings": {
      "api_key": "'"${VOYAGE_API_KEY}"'",
      "model_id": "rerank-2"
    }
  }'
```

### Test Reranking
```bash
curl -X POST "${ES_URL}/_inference/rerank/voyage-rerank-test" \
  -u "${ES_USER}:${ES_PASSWORD}" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is the capital of France?",
    "input": [
      "Paris is the capital of France",
      "London is the capital of England",
      "Berlin is the capital of Germany"
    ]
  }'
```

**Expected**: Returns reranked results with relevance scores

### Cleanup
```bash
curl -X DELETE "${ES_URL}/_inference/rerank/voyage-rerank-test" \
  -u "${ES_USER}:${ES_PASSWORD}"
```

## Verification Checklist

### For Multimodal Models:
- ✅ Model routing detects "voyage-multimodal" prefix
- ✅ Uses `/v1/multimodal-embeddings` endpoint
- ✅ Request JSON uses "inputs" (plural) not "input"
- ✅ Does NOT include `output_dtype` or `output_dimension` parameters
- ✅ Returns valid embeddings

### For Contextual Models:
- ✅ Model routing detects "voyage-context" prefix
- ✅ Uses `/v1/contextualized-embeddings` endpoint
- ✅ Request JSON wraps inputs in nested arrays: `[[str], [str]]`
- ✅ Includes `output_dtype` parameter
- ✅ Response parses nested "embeddings" field correctly
- ✅ Returns flattened list of embeddings

### For Text Models (Regression Test):
- ✅ Uses `/v1/embeddings` endpoint
- ✅ Request JSON uses "input" (singular)
- ✅ Includes both `output_dtype` and `output_dimension` if specified
- ✅ Returns valid embeddings

## Debugging

### Enable Debug Logging
Add to `elasticsearch.yml`:
```yaml
logger.org.elasticsearch.xpack.inference: DEBUG
logger.org.elasticsearch.xpack.inference.external: TRACE
```

### Check Logs
```bash
tail -f /path/to/elasticsearch/logs/elasticsearch.log | grep -i voyage
```

Look for:
- Endpoint selection (multimodal-embeddings vs embeddings vs contextualized-embeddings)
- Request JSON structure
- Response parsing
- Any errors or warnings

## Common Issues

### Issue: "Model not found"
**Solution**: Ensure model ID exactly matches VoyageAI's model names (case-sensitive)

### Issue: "Invalid API key"
**Solution**: Verify `VOYAGE_API_KEY` is correctly set and valid

### Issue: Wrong endpoint used
**Solution**: Check model ID prefix:
- Must start with "voyage-multimodal" for multimodal endpoint
- Must start with "voyage-context" for contextual endpoint
- Others use standard embeddings endpoint

### Issue: Request format error
**Solution**: Check Elasticsearch logs for actual JSON sent to VoyageAI API

## Success Criteria

All tests should:
1. ✅ Create endpoints successfully
2. ✅ Return embeddings without errors
3. ✅ Use correct API endpoints (verify in logs)
4. ✅ Send correctly formatted requests (verify in logs)
5. ✅ Parse responses correctly
6. ✅ Clean up successfully

## Notes

- These tests require an active VoyageAI API key with sufficient credits
- Monitor your API usage during testing
- The contextual model wraps each input string in a list for API compatibility
- Multimodal models do not support `output_dimension` or `output_dtype` parameters per VoyageAI API docs
