/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_NoDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_Success(null, false, null);
    }

    public void testCreateRequest_NoDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_Success(null, true, null);
    }

    public void testCreateRequest_WithDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_Success(384, false, null);
    }

    public void testCreateRequest_WithDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_Success(384, true, 384);
    }

    private void testCreateRequest_Success(Integer dimensions, boolean dimensionsSetByUser, Integer expectedDimensions) throws IOException {
        var request = createRequest(dimensions, dimensionsSetByUser);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), is(List.of("ABCD")));
        assertThat(requestMap.get("model"), is("llama-embed"));
        assertThat(requestMap.get("dimensions"), is(expectedDimensions));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));
    }

    public void testCreateRequest_NoModel_Success() throws IOException {
        var request = createRequest(null, false, null);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), is(List.of("ABCD")));
        assertNull(requestMap.get("model"));
        assertNull(requestMap.get("dimensions"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));

    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest(null, false);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("input"), is(List.of("AB")));
        assertThat(requestMap.get("model"), is("llama-embed"));

    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest(null, false);
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    private HttpPost validateRequestUrlAndContentType(HttpRequest request) {
        assertThat(request.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) request.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is("url"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        return httpPost;
    }

    private static OpenShiftAiEmbeddingsRequest createRequest(Integer dimensions, Boolean dimensionsSetByUser) {
        return createRequest(dimensions, dimensionsSetByUser, "llama-embed");
    }

    private static OpenShiftAiEmbeddingsRequest createRequest(Integer dimensions, Boolean dimensionsSetByUser, String modelId) {
        var embeddingsModel = OpenShiftAiEmbeddingsModelTests.createModel(
            "url",
            "apikey",
            modelId,
            dimensions,
            dimensionsSetByUser,
            dimensions
        );
        return new OpenShiftAiEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel
        );
    }

}
