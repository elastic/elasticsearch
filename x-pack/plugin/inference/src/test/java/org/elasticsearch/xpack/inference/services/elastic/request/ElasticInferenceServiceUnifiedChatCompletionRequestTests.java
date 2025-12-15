/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.EmptySecretSettings;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ElasticInferenceServiceUnifiedChatCompletionRequestTests extends ESTestCase {

    public void testCreateHttpRequest_SingleInput() throws IOException {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var input = "What is 2+2?";

        var request = createRequest(url, modelId, List.of(input), false);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(url + "/api/v1/chat"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("n"), is(1));
        assertThat(requestMap.get("stream"), is(false));
        @SuppressWarnings("unchecked")
        var messages = (List<Map<String, Object>>) requestMap.get("messages");
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).get("content"), is(input));
        assertThat(messages.get(0).get("role"), is("user"));
    }

    public void testCreateHttpRequest_MultipleInputs() throws IOException {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var inputs = List.of("What is 2+2?", "What is the capital of France?");

        var request = createRequest(url, modelId, inputs, false);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        @SuppressWarnings("unchecked")
        var messages = (List<Map<String, Object>>) requestMap.get("messages");
        assertThat(messages.size(), is(2));
        assertThat(messages.get(0).get("content"), is(inputs.get(0)));
        assertThat(messages.get(0).get("role"), is("user"));
        assertThat(messages.get(1).get("content"), is(inputs.get(1)));
        assertThat(messages.get(1).get("role"), is("user"));
    }

    public void testCreateHttpRequest_NonStreaming() throws IOException {
        // Test non-streaming case (used for COMPLETION task type)
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var input = "What is 2+2?";

        var request = createRequest(url, modelId, List.of(input), false);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(false));
        assertFalse(request.isStreaming());
    }

    public void testCreateHttpRequest_Streaming() throws IOException {
        // Test streaming case (used for CHAT_COMPLETION task type)
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var input = "What is 2+2?";

        var request = createRequest(url, modelId, List.of(input), true);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
        assertTrue(request.isStreaming());
    }

    public void testGetURI() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, List.of("input"), false);

        assertThat(request.getURI().toString(), is(url + "/api/v1/chat"));
    }

    public void testGetInferenceEntityId() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var inferenceEntityId = "test-endpoint-id";

        var model = new ElasticInferenceServiceCompletionModel(
            inferenceEntityId,
            TaskType.COMPLETION,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings(modelId),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of(url)
        );

        var unifiedChatInput = new UnifiedChatInput(List.of("input"), "user", false);
        var request = new ElasticInferenceServiceUnifiedChatCompletionRequest(
            unifiedChatInput,
            model,
            new TraceContext("trace-parent", "trace-state"),
            randomElasticInferenceServiceRequestMetadata(),
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );

        assertThat(request.getInferenceEntityId(), is(inferenceEntityId));
    }

    public void testTruncate_ReturnsSameInstance() throws IOException {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var input = "What is 2+2?";

        var request = createRequest(url, modelId, List.of(input), false);
        var truncatedRequest = request.truncate();

        // Should return the same instance (no truncation)
        assertThat(truncatedRequest, is(request));

        // Verify content is unchanged
        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        @SuppressWarnings("unchecked")
        var messages = (List<Map<String, Object>>) requestMap.get("messages");
        assertThat(messages.size(), is(1));
        assertThat(messages.get(0).get("content"), is(input));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, List.of("input"), false);

        assertThat(request.getTruncationInfo(), nullValue());
    }

    public void testIsStreaming_NonStreamingReturnsFalse() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, List.of("input"), false);

        assertFalse(request.isStreaming());
    }

    public void testIsStreaming_StreamingReturnsTrue() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, List.of("input"), true);

        assertTrue(request.isStreaming());
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model-id";
        var traceParent = randomAlphaOfLength(10);
        var traceState = randomAlphaOfLength(10);

        var model = ElasticInferenceServiceCompletionModelTests.createModel(url, modelId);
        var unifiedChatInput = new UnifiedChatInput(List.of("input"), "user", false);
        var request = new ElasticInferenceServiceUnifiedChatCompletionRequest(
            unifiedChatInput,
            model,
            new TraceContext(traceParent, traceState),
            randomElasticInferenceServiceRequestMetadata(),
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );

        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    private ElasticInferenceServiceUnifiedChatCompletionRequest createRequest(
        String url,
        String modelId,
        List<String> inputs,
        boolean stream
    ) {
        var model = ElasticInferenceServiceCompletionModelTests.createModel(url, modelId);
        var unifiedChatInput = new UnifiedChatInput(inputs, "user", stream);

        return new ElasticInferenceServiceUnifiedChatCompletionRequest(
            unifiedChatInput,
            model,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata(),
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
    }
}
