#!/bin/bash

# FireworksAI Integration Test Script
# This script tests the FireworksAI embeddings integration with Elasticsearch

set -e  # Exit on error

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
ES_URL="${ES_URL:-http://localhost:9200}"
FIREWORKS_API_KEY="${FIREWORKS_API_KEY:-fw_3ZkvBpQyjRzbicpihhrihaEP}"

# Model configurations - using serverless Qwen3 embeddings
EMBEDDINGS_MODEL="fireworks/qwen3-embedding-8b"

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}FireworksAI Embeddings Test Script${NC}"
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
    echo -e "Service: $(echo "$EMBEDDINGS_GET_RESPONSE" | jq -r '.endpoints[0].service // .service')"
    echo -e "Model: $(echo "$EMBEDDINGS_GET_RESPONSE" | jq -r '.endpoints[0].service_settings.model_id // .service_settings.model_id')"
fi

# Test 5: List All Inference Endpoints
echo -e "\n${YELLOW}Test 5:${NC} Listing all inference endpoints..."
ALL_ENDPOINTS_RESPONSE=$(curl -s -X GET "$ES_URL/_inference/_all")

if check_response "$ALL_ENDPOINTS_RESPONSE" "List All Endpoints"; then
    ENDPOINT_COUNT=$(echo "$ALL_ENDPOINTS_RESPONSE" | jq '.endpoints | length')
    echo -e "Total endpoints: ${ENDPOINT_COUNT}"
fi

echo -e "\n${BLUE}========================================${NC}"
echo -e "${BLUE}Cleanup${NC}"
echo -e "${BLUE}========================================${NC}\n"

# Cleanup: Delete endpoints
echo -e "${YELLOW}Cleanup:${NC} Deleting test endpoints..."

curl -s -X DELETE "$ES_URL/_inference/text_embedding/fireworks-embeddings" > /dev/null 2>&1
echo -e "${GREEN}✓${NC} Deleted embeddings endpoint"

echo -e "\n${BLUE}========================================${NC}"
echo -e "${GREEN}All tests completed successfully!${NC}"
echo -e "${BLUE}========================================${NC}\n"
