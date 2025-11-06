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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceComponents;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModel;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionModelTests;
import org.elasticsearch.xpack.inference.services.elastic.completion.ElasticInferenceServiceCompletionServiceSettings;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ElasticInferenceServiceCompletionRequestTests extends ESTestCase {

    public void testCreateHttpRequest_SingleInput() throws IOException {
        var url = "http://eis-gateway.com";
        var input = List.of("What is 2+2?");
        var modelId = "my-completion-model";

        var request = createRequest(url, modelId, input);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        // Should contain: model, messages (array), stream, n
        assertThat(requestMap, aMapWithSize(4));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("stream"), is(false));
        assertThat(requestMap.get("n"), is(1));

        @SuppressWarnings("unchecked")
        var messages = (List<Object>) requestMap.get("messages");
        assertThat(messages.size(), is(1));
        @SuppressWarnings("unchecked")
        var message = (java.util.Map<String, Object>) messages.get(0);
        assertThat(message.get("role"), is("user"));
        assertThat(message.get("content"), is("What is 2+2?"));
    }

    public void testCreateHttpRequest_MultipleInputs() throws IOException {
        var url = "http://eis-gateway.com";
        var inputs = List.of("What is 2+2?", "What is the capital of France?");
        var modelId = "my-completion-model";

        var request = createRequest(url, modelId, inputs);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        @SuppressWarnings("unchecked")
        var messages = (List<Object>) requestMap.get("messages");
        assertThat(messages.size(), is(2));

        @SuppressWarnings("unchecked")
        var firstMessage = (java.util.Map<String, Object>) messages.get(0);
        assertThat(firstMessage.get("role"), is("user"));
        assertThat(firstMessage.get("content"), is("What is 2+2?"));

        @SuppressWarnings("unchecked")
        var secondMessage = (java.util.Map<String, Object>) messages.get(1);
        assertThat(secondMessage.get("role"), is("user"));
        assertThat(secondMessage.get("content"), is("What is the capital of France?"));
    }

    public void testCreateHttpRequest_AlwaysNonStreaming() throws IOException {
        var url = "http://eis-gateway.com";
        var input = List.of("test input");
        var modelId = "my-model";

        var request = createRequest(url, modelId, input);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(false));
    }

    public void testGetURI() {
        var url = "http://eis-gateway.com";
        var modelId = "my-model";
        var request = createRequest(url, modelId, List.of("input"));

        var uri = request.getURI();
        assertThat(uri.toString(), is(url + "/api/v1/chat"));
    }

    public void testGetInferenceEntityId() {
        var inferenceEntityId = "test-endpoint-id";
        var model = new ElasticInferenceServiceCompletionModel(
            inferenceEntityId,
            TaskType.COMPLETION,
            "elastic",
            new ElasticInferenceServiceCompletionServiceSettings("model-id"),
            EmptyTaskSettings.INSTANCE,
            EmptySecretSettings.INSTANCE,
            ElasticInferenceServiceComponents.of("http://eis-gateway.com")
        );

        var request = new ElasticInferenceServiceCompletionRequest(
            List.of("input"),
            model,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata()
        );

        assertThat(request.getInferenceEntityId(), is(inferenceEntityId));
    }

    public void testTruncate_ReturnsSameInstance() {
        var request = createRequest("http://eis-gateway.com", "model-id", List.of("input"));
        var truncatedRequest = request.truncate();

        // COMPLETION request doesn't support truncation, should return same instance
        assertThat(truncatedRequest, is(request));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var request = createRequest("http://eis-gateway.com", "model-id", List.of("input"));

        // COMPLETION request doesn't support truncation info
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    public void testIsStreaming_AlwaysReturnsFalse() {
        var request = createRequest("http://eis-gateway.com", "model-id", List.of("input"));

        // COMPLETION adapter always uses non-streaming
        assertThat(request.isStreaming(), is(false));
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var traceParent = randomAlphaOfLength(10);
        var traceState = randomAlphaOfLength(10);
        var traceContext = new TraceContext(traceParent, traceState);

        var model = ElasticInferenceServiceCompletionModelTests.createModel("http://eis-gateway.com", "model-id");
        var request = new ElasticInferenceServiceCompletionRequest(
            List.of("input"),
            model,
            traceContext,
            randomElasticInferenceServiceRequestMetadata()
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(org.elasticsearch.tasks.Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(org.elasticsearch.tasks.Task.TRACE_STATE).getValue(), is(traceState));
    }

    private ElasticInferenceServiceCompletionRequest createRequest(String url, String modelId, List<String> inputs) {
        var model = ElasticInferenceServiceCompletionModelTests.createModel(url, modelId);
        return new ElasticInferenceServiceCompletionRequest(
            inputs,
            model,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata()
        );
    }
}
