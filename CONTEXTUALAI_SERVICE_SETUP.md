# ContextualAI Service for Elasticsearch

This document describes how to build, deploy, and test the ContextualAI inference service integration with Elasticsearch.

## Overview

The ContextualAI service provides document reranking capabilities through Elasticsearch's inference API. This implementation allows users to create inference endpoints that leverage ContextualAI's reranking models.

## Files Modified

### 1. Service Implementation Files

**Created:**
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/ContextualAiService.java`
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/rerank/ContextualAiRerankModel.java`
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/rerank/ContextualAiRerankServiceSettings.java`
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/rerank/ContextualAiRerankTaskSettings.java`
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/response/ContextualAiRerankResponseEntity.java`
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/services/contextualai/request/ContextualAiRerankRequest.java`

**Modified:**
- `x-pack/plugin/inference/src/main/java/org/elasticsearch/xpack/inference/InferencePlugin.java` - Added ContextualAI service to the factory list

### 2. Build Configuration Files

**Modified:**
- `benchmarks/build.gradle` - Fixed annotation processor configuration for Gradle 9.0.0 compatibility

## Build Instructions

### Prerequisites

- Java 21 or higher
- Gradle 9.0.0 (included with project)
- macOS, Linux, or Windows

### Step 1: Fix Build Configuration

The benchmarks annotation processor configuration was updated to resolve Gradle 9.0.0 conflicts:

```gradle
// Removed duplicate annotation processor configuration
// configurations.annotationProcessor.extendsFrom(configurations.compileOnly)
// compileJava.options.compilerArgs += ['-processor', 'org.openjdk.jmh.generators.BenchmarkProcessor']
```

### Step 2: Compile and Build Components

#### Option A: Full Distribution Build (Recommended)
```bash
cd /path/to/elasticsearch
./gradlew :distribution:archives:darwin-tar:assemble --parallel
```

#### Option B: Incremental Component Building

For faster development cycles, you can build specific components:

**Compile Core Elasticsearch:**
```bash
./gradlew :server:compileJava
```

**Compile X-Pack Plugin Framework:**
```bash
./gradlew :x-pack:plugin:core:compileJava
```

**Compile Inference Plugin (includes ContextualAI service):**
```bash
./gradlew :x-pack:plugin:inference:compileJava
```

**Build X-Pack Inference Plugin JAR:**
```bash
./gradlew :x-pack:plugin:inference:jar
```

**Build Complete X-Pack:**
```bash
./gradlew :x-pack:assemble
```

**Build Distribution with X-Pack:**
```bash
./gradlew :distribution:archives:darwin-tar:assemble --parallel
```

#### Option C: Development Build Workflow

For active service development, use this incremental approach:

```bash
# 1. Compile just the inference plugin to check syntax
./gradlew :x-pack:plugin:inference:compileJava

# 2. Run inference plugin tests
./gradlew :x-pack:plugin:inference:test

# 3. Build the plugin JAR
./gradlew :x-pack:plugin:inference:jar

# 4. Build full distribution when ready to test
./gradlew :distribution:archives:darwin-tar:assemble --parallel
```

### Platform-Specific Distribution Targets

Replace `darwin-tar` with the appropriate target for your platform:

| Platform | Target |
|----------|--------|
| macOS | `darwin-tar` |
| Linux | `linux-tar` |
| Windows | `windows-zip` |
| Docker | `docker:assemble` |

### Gradle Build Tasks for ContextualAI Service

**Specific to the ContextualAI service implementation:**

```bash
# Compile only the inference plugin containing ContextualAI service
./gradlew :x-pack:plugin:inference:compileJava

# Run tests for the inference plugin
./gradlew :x-pack:plugin:inference:test

# Check code formatting for the inference plugin
./gradlew :x-pack:plugin:inference:spotlessCheck

# Auto-format code in the inference plugin
./gradlew :x-pack:plugin:inference:spotlessApply

# Build the inference plugin JAR specifically
./gradlew :x-pack:plugin:inference:jar

# Clean and rebuild the inference plugin
./gradlew :x-pack:plugin:inference:clean :x-pack:plugin:inference:jar
```

### Build Verification

After building, verify the ContextualAI service is included:

```bash
# Check if ContextualAI classes are in the JAR
unzip -l build/distributions/elasticsearch-9.2.0-SNAPSHOT-darwin-x86_64.tar.gz | grep -i contextual

# Or after extraction:
find /tmp/elasticsearch-9.2.0-SNAPSHOT -name "*.jar" -exec jar tf {} \; | grep -i contextualai
```

### Step 3: Extract and Deploy

```bash
cd /tmp
rm -rf elasticsearch-9.2.0-SNAPSHOT
tar -xzf /path/to/elasticsearch/distribution/archives/darwin-tar/build/distributions/elasticsearch-9.2.0-SNAPSHOT-darwin-x86_64.tar.gz
```

## Run Instructions

### Step 1: Start Elasticsearch

```bash
cd /tmp/elasticsearch-9.2.0-SNAPSHOT
./bin/elasticsearch
```

### Step 2: Setup Authentication

Reset the elastic user password:
```bash
/tmp/elasticsearch-9.2.0-SNAPSHOT/bin/elasticsearch-reset-password -u elastic --batch
```

Note the generated password for API calls.

### Step 3: Activate Trial License

```bash
curl -k -u elastic:YOUR_PASSWORD -X POST "https://localhost:9200/_license/start_trial?acknowledge=true"
```

## Testing Instructions

### Step 1: Verify Service Registration

Check that ContextualAI service is available:
```bash
curl -k -u elastic:YOUR_PASSWORD "https://localhost:9200/_inference/_services"
```

Look for the `contextualai` service in the response with `rerank` task type.

### Step 2: Create Inference Endpoint

```bash
curl -k -u elastic:YOUR_PASSWORD -X PUT "https://localhost:9200/_inference/rerank/my_contextualai_model" \
  -H "Content-Type: application/json" \
  -d '{
    "service": "contextualai",
    "service_settings": {
      "model_id": "contextual-ai/whale-7b"
    },
    "secret_settings": {
      "api_key": "your-actual-contextualai-api-key"
    }
  }'
```

### Step 3: Test Reranking (with valid API key)

```bash
curl -k -u elastic:YOUR_PASSWORD -X POST "https://localhost:9200/_inference/my_contextualai_model" \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is machine learning?",
    "input": [
      "Machine learning is a subset of artificial intelligence",
      "Python is a programming language",
      "The weather is nice today",
      "Neural networks are used in deep learning"
    ]
  }'
```

## Configuration Options

### Service Settings
- `model_id`: Required. The ContextualAI model identifier
- `url`: Optional. Custom API endpoint URL (defaults to https://api.contextual.ai/v1/rerank)
- `rate_limit.requests_per_minute`: Optional. Rate limiting configuration

### Secret Settings
- `api_key`: Required. Your ContextualAI API key

## Troubleshooting

### Common Issues

1. **License Error**: Ensure trial license is activated
2. **Authentication Error**: Verify elastic user password is correct
3. **SSL Errors**: Use `-k` flag with curl for self-signed certificates
4. **Service Not Found**: Verify the build included the ContextualAI service files

### Validation Errors

- `model_id required`: Add model_id to service_settings
- `api_key required`: Add api_key to secret_settings
- `NullPointerException during validation`: Usually indicates invalid API key or network issues

## Architecture Notes

The ContextualAI service implements Elasticsearch's `RerankingInferenceService` interface and integrates with the standard inference API framework. It supports:

- Document reranking based on query relevance
- Configurable API endpoints
- Rate limiting
- Standard Elasticsearch inference API patterns

The service makes HTTP requests to the ContextualAI API and transforms the responses into Elasticsearch's standard inference result format.