#!/bin/bash

# VoyageAI End-to-End Test Script
# This script tests the VoyageAI integration with real API calls

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
ES_URL="${ES_URL:-http://localhost:9200}"
ES_USER="${ES_USER:-elastic}"
ES_PASSWORD="${ES_PASSWORD:-changeme}"
VOYAGE_API_KEY="${VOYAGE_API_KEY}"

# Check prerequisites
if [ -z "$VOYAGE_API_KEY" ]; then
    echo -e "${RED}Error: VOYAGE_API_KEY environment variable is not set${NC}"
    echo "Usage: export VOYAGE_API_KEY='your-api-key' && ./test-voyageai-e2e.sh"
    exit 1
fi

echo -e "${YELLOW}=== VoyageAI End-to-End Test Suite ===${NC}"
echo "ES URL: $ES_URL"
echo "ES User: $ES_USER"
echo ""

# Test counter
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

# Function to run a test
run_test() {
    local test_name="$1"
    local test_command="$2"
    local expected_pattern="$3"

    TESTS_RUN=$((TESTS_RUN + 1))
    echo -e "${YELLOW}[TEST $TESTS_RUN] $test_name${NC}"

    if output=$(eval "$test_command" 2>&1); then
        if [ -n "$expected_pattern" ]; then
            if echo "$output" | grep -q "$expected_pattern"; then
                echo -e "${GREEN}✓ PASSED${NC}"
                TESTS_PASSED=$((TESTS_PASSED + 1))
                return 0
            else
                echo -e "${RED}✗ FAILED - Expected pattern not found: $expected_pattern${NC}"
                echo "Output: $output"
                TESTS_FAILED=$((TESTS_FAILED + 1))
                return 1
            fi
        else
            echo -e "${GREEN}✓ PASSED${NC}"
            TESTS_PASSED=$((TESTS_PASSED + 1))
            return 0
        fi
    else
        echo -e "${RED}✗ FAILED${NC}"
        echo "Error: $output"
        TESTS_FAILED=$((TESTS_FAILED + 1))
        return 1
    fi
}

# Cleanup function
cleanup() {
    echo -e "\n${YELLOW}Cleaning up test endpoints...${NC}"
    curl -s -X DELETE "${ES_URL}/_inference/text_embedding/voyage-text-test" -u "${ES_USER}:${ES_PASSWORD}" > /dev/null 2>&1 || true
    curl -s -X DELETE "${ES_URL}/_inference/text_embedding/voyage-multimodal-test" -u "${ES_USER}:${ES_PASSWORD}" > /dev/null 2>&1 || true
    curl -s -X DELETE "${ES_URL}/_inference/text_embedding/voyage-contextual-test" -u "${ES_USER}:${ES_PASSWORD}" > /dev/null 2>&1 || true
    curl -s -X DELETE "${ES_URL}/_inference/rerank/voyage-rerank-test" -u "${ES_USER}:${ES_PASSWORD}" > /dev/null 2>&1 || true
    curl -s -X DELETE "${ES_URL}/_inference/text_embedding/voyage-35-test" -u "${ES_USER}:${ES_PASSWORD}" > /dev/null 2>&1 || true
}

# Trap to ensure cleanup on exit
trap cleanup EXIT

# Initial cleanup
cleanup

echo -e "\n${YELLOW}=== Test Suite 1: Text Embeddings (voyage-3) ===${NC}\n"

run_test "Create text embeddings endpoint" \
  "curl -s -X PUT '${ES_URL}/_inference/text_embedding/voyage-text-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"service\": \"voyageai\",
      \"service_settings\": {
        \"api_key\": \"${VOYAGE_API_KEY}\",
        \"model_id\": \"voyage-3\"
      }
    }'" \
  '"model_id":"voyage-3"'

run_test "Get text embeddings endpoint configuration" \
  "curl -s -X GET '${ES_URL}/_inference/text_embedding/voyage-text-test' \
    -u '${ES_USER}:${ES_PASSWORD}'" \
  '"model_id":"voyage-3"'

run_test "Inference with text embeddings" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-text-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Hello world\", \"This is a test\"]
    }'" \
  '"text_embedding"'

run_test "Inference with INGEST input type" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-text-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Document for indexing\"],
      \"task_settings\": {
        \"input_type\": \"ingest\"
      }
    }'" \
  '"text_embedding"'

run_test "Inference with SEARCH input type" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-text-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Search query\"],
      \"task_settings\": {
        \"input_type\": \"search\"
      }
    }'" \
  '"text_embedding"'

echo -e "\n${YELLOW}=== Test Suite 2: Multimodal Embeddings (voyage-multimodal-3) ===${NC}\n"

run_test "Create multimodal embeddings endpoint" \
  "curl -s -X PUT '${ES_URL}/_inference/text_embedding/voyage-multimodal-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"service\": \"voyageai\",
      \"service_settings\": {
        \"api_key\": \"${VOYAGE_API_KEY}\",
        \"model_id\": \"voyage-multimodal-3\"
      }
    }'" \
  '"model_id":"voyage-multimodal-3"'

run_test "Get multimodal embeddings configuration" \
  "curl -s -X GET '${ES_URL}/_inference/text_embedding/voyage-multimodal-test' \
    -u '${ES_USER}:${ES_PASSWORD}'" \
  '"model_id":"voyage-multimodal-3"'

run_test "Inference with multimodal embeddings" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-multimodal-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Multimodal test text\"]
    }'" \
  '"text_embedding"'

echo -e "\n${YELLOW}=== Test Suite 3: Contextual Embeddings (voyage-context-3) ===${NC}\n"

run_test "Create contextual embeddings endpoint" \
  "curl -s -X PUT '${ES_URL}/_inference/text_embedding/voyage-contextual-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"service\": \"voyageai\",
      \"service_settings\": {
        \"api_key\": \"${VOYAGE_API_KEY}\",
        \"model_id\": \"voyage-context-3\"
      }
    }'" \
  '"model_id":"voyage-context-3"'

run_test "Get contextual embeddings configuration" \
  "curl -s -X GET '${ES_URL}/_inference/text_embedding/voyage-contextual-test' \
    -u '${ES_USER}:${ES_PASSWORD}'" \
  '"model_id":"voyage-context-3"'

run_test "Inference with contextual embeddings" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-contextual-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Contextual test sentence\", \"Another test\"]
    }'" \
  '"text_embedding"'

echo -e "\n${YELLOW}=== Test Suite 4: v3.5 Models ===${NC}\n"

run_test "Create voyage-3.5 embeddings endpoint" \
  "curl -s -X PUT '${ES_URL}/_inference/text_embedding/voyage-35-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"service\": \"voyageai\",
      \"service_settings\": {
        \"api_key\": \"${VOYAGE_API_KEY}\",
        \"model_id\": \"voyage-3.5\"
      }
    }'" \
  '"model_id":"voyage-3.5"'

run_test "Inference with voyage-3.5" \
  "curl -s -X POST '${ES_URL}/_inference/text_embedding/voyage-35-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"input\": [\"Test with v3.5 model\"]
    }'" \
  '"text_embedding"'

echo -e "\n${YELLOW}=== Test Suite 5: Rerank (Regression Test) ===${NC}\n"

run_test "Create rerank endpoint" \
  "curl -s -X PUT '${ES_URL}/_inference/rerank/voyage-rerank-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"service\": \"voyageai\",
      \"service_settings\": {
        \"api_key\": \"${VOYAGE_API_KEY}\",
        \"model_id\": \"rerank-2\"
      }
    }'" \
  '"model_id":"rerank-2"'

run_test "Rerank inference" \
  "curl -s -X POST '${ES_URL}/_inference/rerank/voyage-rerank-test' \
    -u '${ES_USER}:${ES_PASSWORD}' \
    -H 'Content-Type: application/json' \
    -d '{
      \"query\": \"What is the capital of France?\",
      \"input\": [
        \"Paris is the capital of France\",
        \"London is the capital of England\",
        \"Berlin is the capital of Germany\"
      ]
    }'" \
  '"rerank"'

# Print summary
echo -e "\n${YELLOW}=== Test Summary ===${NC}"
echo -e "Total tests run: $TESTS_RUN"
echo -e "${GREEN}Tests passed: $TESTS_PASSED${NC}"
if [ $TESTS_FAILED -gt 0 ]; then
    echo -e "${RED}Tests failed: $TESTS_FAILED${NC}"
    exit 1
else
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
fi
