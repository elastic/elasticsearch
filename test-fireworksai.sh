#!/bin/bash

# FireworksAI Integration Test Script
# This script tests the FireworksAI embeddings and reranking integration with Elasticsearch

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ES_URL="${ES_URL:-http://localhost:9200}"
FIREWORKS_API_KEY="fw_3ZkvBpQyjRzbicpihhrihaEP"

# Model configurations
EMBEDDINGS_MODEL="accounts/fireworks/models/qwen3-embedding-8b"
RERANK_MODEL="fireworks/qwen3-reranker-8b"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}FireworksAI Integration Test Script${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Check if API key is provided
if [ -z "$FIREWORKS_API_KEY" ]; then
    echo -e "${RED}ERROR: FIREWORKS_API_KEY environment variable is not set${NC}"
    echo -e "${YELLOW}Usage: FIREWORKS_API_KEY=your_key_here ./test-fireworksai.sh${NC}"
    exit 1
fi

echo -e "${GREEN}✓${NC} API Key found"

# Function to wait for Elasticsearch
wait_for_elasticsearch() {
    echo -e "\n${YELLOW}Waiting for Elasticsearch to be ready...${NC}"
    local max_attempts=60
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s "$ES_URL" > /dev/null 2>&1; then
            echo -e "${GREEN}✓${NC} Elasticsearch is ready!"
            return 0
        fi
        echo -n "."
        sleep 2
        attempt=$((attempt + 1))
    done
    
    echo -e "\n${RED}ERROR: Elasticsearch did not start within 2 minutes${NC}"
    exit 1
}

# Function to check response
check_response() {
    local response=$1
    local test_name=$2
    
    if echo "$response" | jq -e . > /dev/null 2>&1; then
        if echo "$response" | jq -e '.error' > /dev/null 2>&1; then
            echo -e "${RED}✗ FAILED${NC}"
            echo -e "${RED}Error:${NC} $(echo "$response" | jq -r '.error.reason // .error')"
            return 1
        else
            echo -e "${GREEN}✓ PASSED${NC}"
            return 0
        fi
    else
        echo -e "${RED}✗ FAILED${NC}"
        echo -e "${RED}Invalid JSON response${NC}"
        return 1
    fi
}

# Wait for Elasticsearch
wait_for_elasticsearch

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Testing Text Embeddings${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Test 1: Create Embeddings Endpoint
echo -e "${YELLOW}Test 1:${NC} Creating embeddings inference endpoint..."
EMBEDDINGS_CREATE_RESPONSE=$(curl -s -X PUT "$ES_URL/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d "{
  \"service\": \"fireworksai\",
  \"service_settings\": {
    \"api_key\": \"$FIREWORKS_API_KEY\",
    \"model_id\": \"$EMBEDDINGS_MODEL\"
  }
}")

if check_response "$EMBEDDINGS_CREATE_RESPONSE" "Create Embeddings Endpoint"; then
    echo -e "Model: ${EMBEDDINGS_MODEL}"
fi

# Test 2: Generate Embeddings
echo -e "\n${YELLOW}Test 2:${NC} Generating embeddings for text..."
EMBEDDINGS_RESPONSE=$(curl -s -X POST "$ES_URL/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "input": ["Hello world", "Elasticsearch is awesome"]
}')

if check_response "$EMBEDDINGS_RESPONSE" "Generate Embeddings"; then
    EMBEDDING_COUNT=$(echo "$EMBEDDINGS_RESPONSE" | jq '.text_embedding | length')
    EMBEDDING_DIM=$(echo "$EMBEDDINGS_RESPONSE" | jq '.text_embedding[0].embedding | length')
    echo -e "Generated ${EMBEDDING_COUNT} embeddings with ${EMBEDDING_DIM} dimensions"
fi

# Test 3: Embeddings with Dimensions Override
echo -e "\n${YELLOW}Test 3:${NC} Testing embeddings with dimensions override..."
EMBEDDINGS_OVERRIDE_RESPONSE=$(curl -s -X POST "$ES_URL/_inference/text_embedding/fireworks-embeddings" \
-H "Content-Type: application/json" \
-d '{
  "input": ["test dimensions override"],
  "task_settings": {
    "dimensions": 512
  }
}')

if check_response "$EMBEDDINGS_OVERRIDE_RESPONSE" "Embeddings with Dimensions Override"; then
    OVERRIDE_DIM=$(echo "$EMBEDDINGS_OVERRIDE_RESPONSE" | jq '.text_embedding[0].embedding | length')
    echo -e "Override dimensions: ${OVERRIDE_DIM}"
fi

# Test 4: Get Embeddings Endpoint Details
echo -e "\n${YELLOW}Test 4:${NC} Retrieving embeddings endpoint configuration..."
EMBEDDINGS_GET_RESPONSE=$(curl -s -X GET "$ES_URL/_inference/text_embedding/fireworks-embeddings")

if check_response "$EMBEDDINGS_GET_RESPONSE" "Get Embeddings Endpoint"; then
    echo -e "Service: $(echo "$EMBEDDINGS_GET_RESPONSE" | jq -r '.service')"
    echo -e "Model: $(echo "$EMBEDDINGS_GET_RESPONSE" | jq -r '.service_settings.model_id')"
fi

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Testing Reranking${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Test 5: Create Rerank Endpoint
echo -e "${YELLOW}Test 5:${NC} Creating rerank inference endpoint..."
RERANK_CREATE_RESPONSE=$(curl -s -X PUT "$ES_URL/_inference/rerank/fireworks-rerank" \
-H "Content-Type: application/json" \
-d "{
  \"service\": \"fireworksai\",
  \"service_settings\": {
    \"api_key\": \"$FIREWORKS_API_KEY\",
    \"model_id\": \"$RERANK_MODEL\"
  }
}")

if check_response "$RERANK_CREATE_RESPONSE" "Create Rerank Endpoint"; then
    echo -e "Model: ${RERANK_MODEL}"
fi

# Test 6: Rerank Documents
echo -e "\n${YELLOW}Test 6:${NC} Reranking documents..."
RERANK_RESPONSE=$(curl -s -X POST "$ES_URL/_inference/rerank/fireworks-rerank" \
-H "Content-Type: application/json" \
-d '{
  "query": "What is the capital of France?",
  "input": [
    "Paris is the capital and largest city of France",
    "Berlin is the capital of Germany",
    "London is the capital of the United Kingdom",
    "The Eiffel Tower is located in Paris"
  ]
}')

if check_response "$RERANK_RESPONSE" "Rerank Documents"; then
    DOC_COUNT=$(echo "$RERANK_RESPONSE" | jq '.rerank | length')
    TOP_DOC_INDEX=$(echo "$RERANK_RESPONSE" | jq -r '.rerank[0].index')
    TOP_DOC_SCORE=$(echo "$RERANK_RESPONSE" | jq -r '.rerank[0].relevance_score')
    echo -e "Reranked ${DOC_COUNT} documents"
    echo -e "Top document: index=${TOP_DOC_INDEX}, score=${TOP_DOC_SCORE}"
fi

# Test 7: Rerank with Top-N Limit
echo -e "\n${YELLOW}Test 7:${NC} Testing rerank with top_n limit..."
RERANK_TOPN_RESPONSE=$(curl -s -X POST "$ES_URL/_inference/rerank/fireworks-rerank" \
-H "Content-Type: application/json" \
-d '{
  "query": "machine learning",
  "input": [
    "Deep learning is a subset of machine learning",
    "Python is a programming language",
    "Neural networks are used in AI",
    "JavaScript is for web development",
    "TensorFlow is a machine learning framework"
  ],
  "task_settings": {
    "top_n": 2,
    "return_documents": true
  }
}')

if check_response "$RERANK_TOPN_RESPONSE" "Rerank with Top-N"; then
    RETURNED_COUNT=$(echo "$RERANK_TOPN_RESPONSE" | jq '.rerank | length')
    echo -e "Returned top ${RETURNED_COUNT} documents"
fi

# Test 8: Get Rerank Endpoint Details
echo -e "\n${YELLOW}Test 8:${NC} Retrieving rerank endpoint configuration..."
RERANK_GET_RESPONSE=$(curl -s -X GET "$ES_URL/_inference/rerank/fireworks-rerank")

if check_response "$RERANK_GET_RESPONSE" "Get Rerank Endpoint"; then
    echo -e "Service: $(echo "$RERANK_GET_RESPONSE" | jq -r '.service')"
    echo -e "Model: $(echo "$RERANK_GET_RESPONSE" | jq -r '.service_settings.model_id')"
fi

# Test 9: List All Inference Endpoints
echo -e "\n${YELLOW}Test 9:${NC} Listing all inference endpoints..."
ALL_ENDPOINTS_RESPONSE=$(curl -s -X GET "$ES_URL/_inference/_all")

if check_response "$ALL_ENDPOINTS_RESPONSE" "List All Endpoints"; then
    ENDPOINT_COUNT=$(echo "$ALL_ENDPOINTS_RESPONSE" | jq '. | length')
    echo -e "Total endpoints: ${ENDPOINT_COUNT}"
fi

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Cleanup${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Cleanup: Delete endpoints
echo -e "${YELLOW}Cleanup:${NC} Deleting test endpoints..."

curl -s -X DELETE "$ES_URL/_inference/text_embedding/fireworks-embeddings" > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Deleted embeddings endpoint"

curl -s -X DELETE "$ES_URL/_inference/rerank/fireworks-rerank" > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Deleted rerank endpoint"

echo -e "\n${BLUE}========================================${NC}"
echo -e "${GREEN}All tests completed successfully!${NC}"
echo -e "${BLUE}========================================${NC}\n"

