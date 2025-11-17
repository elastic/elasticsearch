/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.cohere.CohereTruncation;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModelTests;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class NvidiaEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_AllFields_InputTypeFromRequest_Success() throws IOException {
        var request = createRequest("url", "some model", InputType.SEARCH, CohereTruncation.START, InputType.INGEST);
        testCreateRequest_AllFields(request);
    }

    public void testCreateRequest_AllFields_InputTypeFromTaskSettings_Success() throws IOException {
        var request = createRequest("url", "some model", InputType.INGEST, CohereTruncation.START, null);
        testCreateRequest_AllFields(request);
    }

    private void testCreateRequest_AllFields(NvidiaEmbeddingsRequest request) throws IOException {
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "url");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), Matchers.is(List.of("ABCD")));
        assertThat(requestMap.get("model"), Matchers.is("some model"));
        assertThat(requestMap.get("input_type"), Matchers.is("passage"));
        assertThat(requestMap.get("truncate"), Matchers.is("start"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), Matchers.is("Bearer apikey"));
    }

    public void testCreateRequest_DefaultUrl_Success() throws IOException {
        var request = createRequest(null, "some model");
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest, "https://integrate.api.nvidia.com/v1/embeddings");

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), Matchers.is(List.of("ABCD")));
        assertThat(requestMap.get("model"), Matchers.is("some model"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), Matchers.is("Bearer apikey"));
    }

    public void testCreateRequest_NoModel_ThrowsException() {
        expectThrows(NullPointerException.class, () -> createRequest("url", null));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest("url", "some model");
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), Matchers.is(List.of("AB")));
        assertThat(requestMap.get("model"), Matchers.is("some model"));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest("url", "some model");
        assertThat(request.getTruncationInfo()[0], is(false));

        var truncatedRequest = request.truncate();
        assertThat(truncatedRequest.getTruncationInfo()[0], is(true));
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), Matchers.is(expectedUrl));
        assertThat(
            httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(),
            Matchers.is(XContentType.JSON.mediaTypeWithoutParameters())
        );
        return httpPost;
    }

    private static NvidiaEmbeddingsRequest createRequest(@Nullable String url, @Nullable String modelId) {
        return createRequest(url, modelId, null, null, null);
    }

    private static NvidiaEmbeddingsRequest createRequest(
        @Nullable String url,
        @Nullable String modelId,
        @Nullable InputType inputType,
        @Nullable CohereTruncation truncation,
        @Nullable InputType requestInputType
    ) {
        var embeddingsModel = NvidiaEmbeddingsModelTests.createModel(url, "apikey", modelId, 123, 1536, inputType, truncation);
        return new NvidiaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel,
            requestInputType
        );
    }

}
