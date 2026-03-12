/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.elastic.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.InferenceStringGroup;
import org.elasticsearch.inference.InferenceStringGroupTests;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class ElasticInferenceServiceDenseEmbeddingsRequestTests extends ESTestCase {

    public static final String URL = "http://eis-gateway.com";
    public static final String MODEL_ID = "my-dense-model-id";

    public void testCreateHttpRequest_UsageContextSearch_TextEmbedding() throws IOException {
        testCreateHttpRequest(TEXT_EMBEDDING, randomFrom(InputType.SEARCH, InputType.INTERNAL_SEARCH));
    }

    public void testCreateHttpRequest_UsageContextSearch_Embedding() throws IOException {
        testCreateHttpRequest(EMBEDDING, randomFrom(InputType.SEARCH, InputType.INTERNAL_SEARCH));
    }

    public void testCreateHttpRequest_UsageContextIngest_TextEmbedding() throws IOException {
        testCreateHttpRequest(TEXT_EMBEDDING, randomFrom(InputType.INGEST, InputType.INTERNAL_INGEST));
    }

    public void testCreateHttpRequest_UsageContextIngest_Embedding() throws IOException {
        testCreateHttpRequest(EMBEDDING, randomFrom(InputType.INGEST, InputType.INTERNAL_INGEST));
    }

    public void testCreateHttpRequest_UsageContextUnspecified_TextEmbedding() throws IOException {
        testCreateHttpRequest(TEXT_EMBEDDING, InputType.UNSPECIFIED);
    }

    public void testCreateHttpRequest_UsageContextUnspecified_Embedding() throws IOException {
        testCreateHttpRequest(EMBEDDING, InputType.UNSPECIFIED);
    }

    private void testCreateHttpRequest(TaskType taskType, InputType inputType) throws IOException {
        var input = List.of(new InferenceStringGroup("input text"));

        var request = createRequest(taskType, input, inputType);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        if (inputType == InputType.UNSPECIFIED) {
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap, not(hasKey("usage_context")));
        } else {
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("usage_context"), is(inputTypeToUsageContext(inputType)));
        }

        if (taskType == TEXT_EMBEDDING) {
            assertThat(request.getURI().getPath(), endsWith("/embed/text/dense"));
            assertThat(requestMap.get("input"), is(InferenceStringGroup.toStringList(input)));
        } else {
            assertThat(request.getURI().getPath(), endsWith("/embed/dense"));
            assertThat(requestMap.get("input"), is(input.stream().map(InferenceStringGroupTests::toRequestMap).toList()));
        }

        assertThat(requestMap.get("model"), is(MODEL_ID));
    }

    private static String inputTypeToUsageContext(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> "ingest";
            case SEARCH, INTERNAL_SEARCH -> "search";
            default -> "";
        };
    }

    public void testCreateHttpRequest_MultipleInputs_TextEmbedding() throws IOException {
        testCreateHttpRequest_MultipleInputs(TEXT_EMBEDDING);
    }

    public void testCreateHttpRequest_MultipleInputs_Embedding() throws IOException {
        testCreateHttpRequest_MultipleInputs(EMBEDDING);
    }

    private void testCreateHttpRequest_MultipleInputs(TaskType taskType) throws IOException {
        var inputs = List.of(
            new InferenceStringGroup("first input"),
            new InferenceStringGroup("second input"),
            new InferenceStringGroup("third input")
        );

        var request = createRequest(taskType, inputs, InputType.SEARCH);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), is(3));
        if (taskType == TEXT_EMBEDDING) {
            assertThat(request.getURI().getPath(), endsWith("/embed/text/dense"));
            assertThat(requestMap.get("input"), is(InferenceStringGroup.toStringList(inputs)));
        } else {
            assertThat(request.getURI().getPath(), endsWith("/embed/dense"));
            assertThat(requestMap.get("input"), is(inputs.stream().map(InferenceStringGroupTests::toRequestMap).toList()));
        }
        assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap.get("usage_context"), is("search"));
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var input = List.of(new InferenceStringGroup("input text"));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, InputType.UNSPECIFIED);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testTruncate_ReturnsSameInstance() {
        var input = List.of(new InferenceStringGroup("input text"));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, randomFrom(InputType.UNSPECIFIED));
        var truncatedRequest = request.truncate();

        // Dense embeddings request doesn't support truncation, should return same instance
        assertThat(truncatedRequest, sameInstance(request));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var input = List.of(new InferenceStringGroup("input text"));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, InputType.UNSPECIFIED);

        // Dense embeddings request doesn't support truncation info
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    public void testDecorate_HttpRequest_WithProductUseCase() {
        var input = new InferenceStringGroup("elastic");

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseEmbeddingsRequest(
                ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(randomFrom(TEXT_EMBEDDING, EMBEDDING), URL, MODEL_ID),
                List.of(input),
                new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
                new ElasticInferenceServiceRequestMetadata("my-product-origin", "my-product-use-case-from-metadata", "1.2.3"),
                inputType,
                CCMAuthenticationApplierFactory.NOOP_APPLIER
            );

            var httpRequest = request.createHttpRequest();

            assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
            var httpPost = (HttpPost) httpRequest.httpRequestBase();

            var headers = httpPost.getHeaders(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
            assertThat(headers.length, is(2));
            assertThat(headers[0].getValue(), is(inputType.toString()));
            assertThat(headers[1].getValue(), is("my-product-use-case-from-metadata"));
        }
    }

    public void testDecorate_HttpRequest_WithAuthorizationHeader() {
        var input = new InferenceStringGroup("elastic");
        var secret = "secret";

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseEmbeddingsRequest(
                ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(randomFrom(TEXT_EMBEDDING, EMBEDDING), URL, MODEL_ID),
                List.of(input),
                new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
                new ElasticInferenceServiceRequestMetadata("my-product-origin", "my-product-use-case-from-metadata", "1.2.3"),
                inputType,
                new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(new SecureString(secret.toCharArray()))
            );

            var httpRequest = request.createHttpRequest();

            assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
            var httpPost = (HttpPost) httpRequest.httpRequestBase();

            var headers = httpPost.getHeaders(HttpHeaders.AUTHORIZATION);
            assertThat(headers.length, is(1));
            assertThat(headers[0].getValue(), is(apiKey(secret)));
        }
    }

    private ElasticInferenceServiceDenseEmbeddingsRequest createRequest(
        TaskType taskType,
        List<InferenceStringGroup> inputs,
        InputType inputType
    ) {
        var embeddingsModel = ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(taskType, URL, MODEL_ID);

        return new ElasticInferenceServiceDenseEmbeddingsRequest(
            embeddingsModel,
            inputs,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata(),
            inputType,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
    }
}
