/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.elastic;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSparseEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceUsageContext;
import org.elasticsearch.xpack.inference.telemetry.TraceContext;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.request.elastic.ElasticInferenceServiceSparseEmbeddingsRequest.inputTypeToUsageContext;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ElasticInferenceServiceSparseEmbeddingsRequestTests extends ESTestCase {

    public void testCreateHttpRequest_UsageContextSearch() throws IOException {
        var url = "http://eis-gateway.com";
        var input = "input";

        var request = createRequest(url, input, InputType.SEARCH);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), equalTo(2));
        assertThat(requestMap.get("input"), is(List.of(input)));
        assertThat(requestMap.get("usage_context"), equalTo("search"));
    }

    public void testTraceContextPropagatedThroughHTTPHeaders() {
        var url = "http://eis-gateway.com";
        var input = "input";

        var request = createRequest(url, input, InputType.UNSPECIFIED);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var traceParent = request.getTraceContext().traceParent();
        var traceState = request.getTraceContext().traceState();

        assertThat(httpPost.getLastHeader(Task.TRACE_PARENT_HTTP_HEADER).getValue(), is(traceParent));
        assertThat(httpPost.getLastHeader(Task.TRACE_STATE).getValue(), is(traceState));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var url = "http://eis-gateway.com";
        var input = "abcd";

        var request = createRequest(url, input, InputType.UNSPECIFIED);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(1));
        assertThat(requestMap.get("input"), is(List.of("ab")));
    }

    public void testIsTruncated_ReturnsTrue() {
        var url = "http://eis-gateway.com";
        var input = "abcd";

        var request = createRequest(url, input, InputType.UNSPECIFIED);
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public void testInputTypeToUsageContext_Search() {
        assertThat(inputTypeToUsageContext(InputType.SEARCH), equalTo(ElasticInferenceServiceUsageContext.SEARCH));
    }

    public void testInputTypeToUsageContext_Ingest() {
        assertThat(inputTypeToUsageContext(InputType.INGEST), equalTo(ElasticInferenceServiceUsageContext.INGEST));
    }

    public void testInputTypeToUsageContext_Unspecified() {
        assertThat(inputTypeToUsageContext(InputType.UNSPECIFIED), equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED));
    }

    public void testInputTypeToUsageContext_Unknown_DefaultToUnspecified() {
        assertThat(inputTypeToUsageContext(InputType.CLASSIFICATION), equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED));
        assertThat(inputTypeToUsageContext(InputType.CLUSTERING), equalTo(ElasticInferenceServiceUsageContext.UNSPECIFIED));
    }

    public ElasticInferenceServiceSparseEmbeddingsRequest createRequest(String url, String input, InputType inputType) {
        var embeddingsModel = ElasticInferenceServiceSparseEmbeddingsModelTests.createModel(url);

        return new ElasticInferenceServiceSparseEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            embeddingsModel,
            new TraceContext(randomAlphaOfLength(10), randomAlphaOfLength(10)),
            inputType
        );
    }
}
