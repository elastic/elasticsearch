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
import org.elasticsearch.inference.telemetry.InferenceProductContext;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.denseembeddings.ElasticInferenceServiceDenseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.inference.TaskType.EMBEDDING;
import static org.elasticsearch.inference.TaskType.TEXT_EMBEDDING;
import static org.elasticsearch.inference.telemetry.InferenceProductContext.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
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

    private static final String TEST_URL = "http://eis-gateway.com";
    private static final String TEST_MODEL_ID = "my-dense-model-id";
    private static final String TEST_INPUT_TEXT = "input text";
    private static final String TEST_ELASTIC_INPUT = "elastic";
    private static final String TEST_SECRET = "secret";
    private static final String TEST_PRODUCT_USE_CASE = "my-product-use-case-from-metadata";
    private static final String TEST_PRODUCT_ORIGIN = "my-product-origin";
    private static final String TEST_ES_VERSION = "1.2.3";
    private static final String TEST_INPUT_FIELD = "input";
    private static final String TEST_MODEL_FIELD = "model";
    private static final String TEST_USAGE_CONTEXT_FIELD = "usage_context";
    private static final String TEST_TEXT_DENSE_PATH_SUFFIX = "/embed/text/dense";
    private static final String TEST_DENSE_PATH_SUFFIX = "/embed/dense";
    private static final String TEST_SEARCH_USAGE_CONTEXT = "search";
    private static final String TEST_INGEST_USAGE_CONTEXT = "ingest";

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
        var input = List.of(new InferenceStringGroup(TEST_INPUT_TEXT));

        var request = createRequest(taskType, input, inputType);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        if (inputType == InputType.UNSPECIFIED) {
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap, not(hasKey(TEST_USAGE_CONTEXT_FIELD)));
        } else {
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get(TEST_USAGE_CONTEXT_FIELD), is(inputTypeToUsageContext(inputType)));
        }

        if (taskType == TEXT_EMBEDDING) {
            assertThat(request.getURI().getPath(), endsWith(TEST_TEXT_DENSE_PATH_SUFFIX));
            assertThat(requestMap.get(TEST_INPUT_FIELD), is(InferenceStringGroup.toStringList(input)));
        } else {
            assertThat(request.getURI().getPath(), endsWith(TEST_DENSE_PATH_SUFFIX));
            assertThat(requestMap.get(TEST_INPUT_FIELD), is(input.stream().map(InferenceStringGroupTests::toRequestMap).toList()));
        }

        assertThat(requestMap.get(TEST_MODEL_FIELD), is(TEST_MODEL_ID));
    }

    private static String inputTypeToUsageContext(InputType inputType) {
        return switch (inputType) {
            case INGEST, INTERNAL_INGEST -> TEST_INGEST_USAGE_CONTEXT;
            case SEARCH, INTERNAL_SEARCH -> TEST_SEARCH_USAGE_CONTEXT;
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
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), is(3));
        if (taskType == TEXT_EMBEDDING) {
            assertThat(request.getURI().getPath(), endsWith(TEST_TEXT_DENSE_PATH_SUFFIX));
            assertThat(requestMap.get(TEST_INPUT_FIELD), is(InferenceStringGroup.toStringList(inputs)));
        } else {
            assertThat(request.getURI().getPath(), endsWith(TEST_DENSE_PATH_SUFFIX));
            assertThat(requestMap.get(TEST_INPUT_FIELD), is(inputs.stream().map(InferenceStringGroupTests::toRequestMap).toList()));
        }
        assertThat(requestMap.get(TEST_MODEL_FIELD), is(TEST_MODEL_ID));
        assertThat(requestMap.get(TEST_USAGE_CONTEXT_FIELD), is(TEST_SEARCH_USAGE_CONTEXT));
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var input = List.of(new InferenceStringGroup(TEST_INPUT_TEXT));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, InputType.UNSPECIFIED);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testTruncate_ReturnsSameInstance() {
        var input = List.of(new InferenceStringGroup(TEST_INPUT_TEXT));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, randomFrom(InputType.UNSPECIFIED));
        var truncatedRequest = request.truncate();

        // Dense embeddings request doesn't support truncation, should return same instance
        assertThat(truncatedRequest, sameInstance(request));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var input = List.of(new InferenceStringGroup(TEST_INPUT_TEXT));

        var request = createRequest(randomFrom(TEXT_EMBEDDING, EMBEDDING), input, InputType.UNSPECIFIED);

        // Dense embeddings request doesn't support truncation info
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    public void testDecorate_HttpRequest_WithProductUseCase() {
        var input = new InferenceStringGroup(TEST_ELASTIC_INPUT);

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseEmbeddingsRequest(
                ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(
                    randomFrom(TEXT_EMBEDDING, EMBEDDING),
                    TEST_URL,
                    TEST_MODEL_ID
                ),
                List.of(input),
                new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
                new ElasticInferenceServiceRequestMetadata(
                    new InferenceProductContext(TEST_PRODUCT_USE_CASE, TEST_PRODUCT_ORIGIN),
                    TEST_ES_VERSION
                ),
                inputType,
                CCMAuthenticationApplierFactory.NOOP_APPLIER
            );

            var httpRequest = RequestTests.getHttpRequestSync(request);

            assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
            var httpPost = (HttpPost) httpRequest.httpRequestBase();

            var headers = httpPost.getHeaders(X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER);
            assertThat(headers.length, is(2));
            assertThat(headers[0].getValue(), is(inputType.toString()));
            assertThat(headers[1].getValue(), is(TEST_PRODUCT_USE_CASE));
        }
    }

    public void testDecorate_HttpRequest_WithAuthorizationHeader() {
        var input = new InferenceStringGroup(TEST_ELASTIC_INPUT);

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseEmbeddingsRequest(
                ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(
                    randomFrom(TEXT_EMBEDDING, EMBEDDING),
                    TEST_URL,
                    TEST_MODEL_ID
                ),
                List.of(input),
                new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
                new ElasticInferenceServiceRequestMetadata(
                    new InferenceProductContext(TEST_PRODUCT_USE_CASE, TEST_PRODUCT_ORIGIN),
                    TEST_ES_VERSION
                ),
                inputType,
                new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(new SecureString(TEST_SECRET.toCharArray()))
            );

            var httpRequest = RequestTests.getHttpRequestSync(request);

            assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
            var httpPost = (HttpPost) httpRequest.httpRequestBase();

            var headers = httpPost.getHeaders(HttpHeaders.AUTHORIZATION);
            assertThat(headers.length, is(1));
            assertThat(headers[0].getValue(), is(apiKey(TEST_SECRET)));
        }
    }

    private ElasticInferenceServiceDenseEmbeddingsRequest createRequest(
        TaskType taskType,
        List<InferenceStringGroup> inputs,
        InputType inputType
    ) {
        var embeddingsModel = ElasticInferenceServiceDenseEmbeddingsModelTests.createModel(taskType, TEST_URL, TEST_MODEL_ID);

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
