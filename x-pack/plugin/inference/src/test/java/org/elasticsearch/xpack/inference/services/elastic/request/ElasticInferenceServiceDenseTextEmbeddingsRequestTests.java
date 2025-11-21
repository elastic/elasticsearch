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
import org.elasticsearch.inference.InputType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.densetextembeddings.ElasticInferenceServiceDenseTextEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.InferencePlugin.X_ELASTIC_PRODUCT_USE_CASE_HTTP_HEADER;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ElasticInferenceServiceDenseTextEmbeddingsRequestTests extends ESTestCase {

    public void testCreateHttpRequest_UsageContextSearch() throws IOException {
        var url = "http://eis-gateway.com";
        var input = List.of("input text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.SEARCH);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(3));
        assertThat(requestMap.get("input"), is(input));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("usage_context"), equalTo("search"));
    }

    public void testCreateHttpRequest_UsageContextIngest() throws IOException {
        var url = "http://eis-gateway.com";
        var input = List.of("ingest text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.INGEST);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(3));
        assertThat(requestMap.get("input"), is(input));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("usage_context"), equalTo("ingest"));
    }

    public void testCreateHttpRequest_UsageContextUnspecified() throws IOException {
        var url = "http://eis-gateway.com";
        var input = List.of("unspecified text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.UNSPECIFIED);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(input));
        assertThat(requestMap.get("model"), is(modelId));
        // usage_context should not be present for UNSPECIFIED
    }

    public void testCreateHttpRequest_MultipleInputs() throws IOException {
        var url = "http://eis-gateway.com";
        var inputs = List.of("first input", "second input", "third input");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, inputs, InputType.SEARCH);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(3));
        assertThat(requestMap.get("input"), is(inputs));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("usage_context"), equalTo("search"));
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var url = "http://eis-gateway.com";
        var input = List.of("input text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.UNSPECIFIED);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testTruncate_ReturnsSameInstance() {
        var url = "http://eis-gateway.com";
        var input = List.of("input text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.UNSPECIFIED);
        var truncatedRequest = request.truncate();

        // Dense text embeddings request doesn't support truncation, should return same instance
        assertThat(truncatedRequest, is(request));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var url = "http://eis-gateway.com";
        var input = List.of("input text");
        var modelId = "my-dense-model-id";

        var request = createRequest(url, modelId, input, InputType.UNSPECIFIED);

        // Dense text embeddings request doesn't support truncation info
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    public void testDecorate_HttpRequest_WithProductUseCase() {
        var input = "elastic";
        var modelId = "my-model-id";
        var url = "http://eis-gateway.com";

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseTextEmbeddingsRequest(
                ElasticInferenceServiceDenseTextEmbeddingsModelTests.createModel(url, modelId),
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
        var input = "elastic";
        var modelId = "my-model-id";
        var url = "http://eis-gateway.com";
        var secret = "secret";

        for (var inputType : List.of(InputType.INTERNAL_SEARCH, InputType.INTERNAL_INGEST, InputType.UNSPECIFIED)) {
            var request = new ElasticInferenceServiceDenseTextEmbeddingsRequest(
                ElasticInferenceServiceDenseTextEmbeddingsModelTests.createModel(url, modelId),
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
            assertThat(headers[0].getValue(), is(bearerToken(secret)));
        }
    }

    private ElasticInferenceServiceDenseTextEmbeddingsRequest createRequest(
        String url,
        String modelId,
        List<String> inputs,
        InputType inputType
    ) {
        var embeddingsModel = ElasticInferenceServiceDenseTextEmbeddingsModelTests.createModel(url, modelId);

        return new ElasticInferenceServiceDenseTextEmbeddingsRequest(
            embeddingsModel,
            inputs,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata(),
            inputType,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
    }
}
