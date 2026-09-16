/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.DataFormat;
import org.elasticsearch.inference.DataType;
import org.elasticsearch.inference.InferenceString;
import org.elasticsearch.inference.InferenceStringTests;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMAuthenticationApplierFactory;
import org.elasticsearch.xpack.inference.services.elastic.documentextraction.ElasticInferenceServiceDocumentExtractionModelTests;
import org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceDocumentExtractionRequest;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequestTests.randomElasticInferenceServiceRequestMetadata;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceDocumentExtractionRequestTests extends ESTestCase {

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var url = "http://eis-gateway.com";
        var documents = List.of(randomPdfDocument());
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, documents);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testCreatesExpectedRequestBody() throws IOException {
        var url = "http://eis-gateway.com";
        var documents = List.of(randomPdfDocument(), randomPdfDocument());
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, documents);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(
            requestMap.get("input"),
            is(documents.stream().map(document -> Map.of("content", InferenceStringTests.inferenceStringToMap(document))).toList())
        );
    }

    public void testTruncate_DoesNotTruncate() {
        var url = "http://eis-gateway.com";
        var documents = List.of(randomPdfDocument());
        var modelId = "my-model-id";

        var request = createRequest(url, modelId, documents);
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, is(request));
    }

    public void testDecorate_HttpRequest_WithAuthorizationHeader() {
        var url = "http://eis-gateway.com";
        var documents = List.of(randomPdfDocument());
        var modelId = "my-model-id";
        var secret = "secret";

        var request = new ElasticInferenceServiceDocumentExtractionRequest(
            documents,
            ElasticInferenceServiceDocumentExtractionModelTests.createModel(url, modelId),
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata(),
            null,
            new CCMAuthenticationApplierFactory.AuthenticationHeaderApplier(secret)
        );
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var headers = httpPost.getHeaders(HttpHeaders.AUTHORIZATION);
        assertThat(headers.length, is(1));
        assertThat(headers[0].getValue(), is(apiKey(secret)));
    }

    private static InferenceString randomPdfDocument() {
        return new InferenceString(DataType.PDF, DataFormat.BASE64, "data:application/pdf;base64," + randomAlphanumericOfLength(16));
    }

    private ElasticInferenceServiceDocumentExtractionRequest createRequest(String url, String modelId, List<InferenceString> documents) {
        var model = ElasticInferenceServiceDocumentExtractionModelTests.createModel(url, modelId);

        return new ElasticInferenceServiceDocumentExtractionRequest(
            documents,
            model,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            randomElasticInferenceServiceRequestMetadata(),
            null,
            CCMAuthenticationApplierFactory.NOOP_APPLIER
        );
    }

}
