# Add ContextualAI Rerank Service Implementation

## Overview
This PR adds a complete implementation of ContextualAI rerank service integration to Elasticsearch's inference plugin, following the established patterns used by other inference services (OpenAI, Cohere, etc.).

## What's Implemented
- **Complete ContextualAI Service**: Full service implementation with rerank task support
- **Service Registration**: Integrated with InferencePlugin to register the service
- **Request/Response Handling**: Proper HTTP client integration with ContextualAI API
- **Configuration Management**: Service settings, task settings, and secret handling
- **Rate Limiting**: Integrated with Elasticsearch's rate limiting infrastructure
- **Instruction Support**: Added instruction parameter for guiding rerank behavior

## Files Added/Modified
- `ContextualAiService.java` - Main service implementation
- `ContextualAiRerankModel.java` - Model representation
- `ContextualAiRerankServiceSettings.java` - Service configuration
- `ContextualAiRerankTaskSettings.java` - Task-specific settings with instruction support
- `ContextualAiActionCreator.java` - Action creation logic
- `ContextualAiRerankRequest.java` - HTTP request handling with debug logging
- `ContextualAiRerankRequestEntity.java` - JSON serialization with proper field ordering
- `ContextualAiRerankResponseEntity.java` - Response parsing
- `InferencePlugin.java` - Service registration

## Current Status: BLOCKED - Need Help

### Problem
The implementation compiles successfully and the service registers correctly, but I'm encountering validation issues during inference execution (POST requests). Specifically:

```
"error": {
    "type": "validation_exception", 
    "reason": "Validation Failed: 1: [service_settings] does not contain the required setting [model_id];"
}
```

### Root Cause Analysis
The issue appears to be in how `parseRequestConfig` is being called during POST inference requests. The system is incorrectly trying to validate service settings during inference execution when it should only validate them during endpoint registration (PUT requests).

**What I've tried:**
1. ✅ Fixed `ContextualAiRerankServiceSettings.fromMap()` to handle REQUEST vs PERSISTENT contexts differently
2. ✅ Made `model_id` optional for REQUEST context (following Cohere service pattern)
3. ✅ Updated constructor to handle nullable model_id
4. ✅ Added proper debug logging to trace the request flow
5. ❌ Still getting validation errors on POST requests

### What I Need Help With

1. **Architecture Guidance**: 
   - Should `parseRequestConfig` be called at all for simple inference requests without task setting overrides?
   - Is there a different code path that should be taken for POST vs PUT requests?

2. **Pattern Clarification**:
   - How do other services (OpenAI, Cohere) handle the distinction between registration and inference?
   - Is there a specific interface method I should implement differently?

3. **Debugging Assistance**:
   - Where in the inference pipeline should I set breakpoints to trace why `parseRequestConfig` is being called?
   - Are there specific validation steps I'm missing?

### Expected Behavior
**Registration (PUT)** should work:
```json
PUT /_inference/rerank/contextualai-reranker
{
  "service": "contextualai",
  "service_settings": { "model_id": "ctxl-rerank-v2-instruct-multilingual" },
  "secrets": { "api_key": "..." }
}
```

**Inference (POST)** should work:
```json
POST /_inference/rerank/contextualai-reranker  
{
  "query": "search query",
  "input": ["doc1", "doc2", "doc3"]
}
```

### Additional Context
- All ContextualAI files follow established patterns from other services
- Service compiles and registers successfully
- Debug logging shows proper JSON formatting for ContextualAI API
- Authentication headers are properly configured
- Rate limiting infrastructure is in place

**Help needed**: Guidance on the proper inference request handling pattern and why validation is failing during POST requests.