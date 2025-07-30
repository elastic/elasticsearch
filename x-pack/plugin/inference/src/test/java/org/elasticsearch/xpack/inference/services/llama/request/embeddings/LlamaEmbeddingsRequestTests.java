/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.embeddings;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithAuth_WithUser_NoDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_WithAuth_Success("user", null, false, null);
    }

    public void testCreateRequest_WithAuth_WithUser_NoDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_WithAuth_Success("user", null, true, null);
    }

    public void testCreateRequest_WithAuth_WithUser_WithDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_WithAuth_Success("user", 384, false, null);
    }

    public void testCreateRequest_WithAuth_WithUser_WithDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_WithAuth_Success("user", 384, true, 384);
    }

    public void testCreateRequest_WithAuth_NoUser_NoDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_WithAuth_Success(null, null, false, null);
    }

    public void testCreateRequest_WithAuth_NoUser_NoDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_WithAuth_Success(null, null, true, null);
    }

    public void testCreateRequest_WithAuth_NoUser_WithDimensions_DimensionsSetByUserFalse_Success() throws IOException {
        testCreateRequest_WithAuth_Success(null, 384, false, null);
    }

    public void testCreateRequest_WithAuth_NoUser_WithDimensions_DimensionsSetByUserTrue_Success() throws IOException {
        testCreateRequest_WithAuth_Success(null, 384, true, 384);
    }

    private void testCreateRequest_WithAuth_Success(
        String user,
        Integer dimensions,
        boolean dimensionsSetByUser,
        Integer expectedDimensions
    ) throws IOException {
        var request = createRequest(user, dimensions, dimensionsSetByUser);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("input"), is(List.of("ABCD")));
        assertThat(requestMap.get("model"), is("llama-embed"));
        assertThat(requestMap.get("dimensions"), is(expectedDimensions));
        assertThat(requestMap.get("user"), is(user));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));
    }

    public void testCreateRequest_NoAuth_Success() throws IOException {
        testCreateRequest_NoAuth_Success("user");
    }

    public void testCreateRequest_NoAuth_NoUser_Success() throws IOException {
        testCreateRequest_NoAuth_Success(null);
    }

    private void testCreateRequest_NoAuth_Success(String user) throws IOException {
        var request = createRequestNoAuth(user);
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (user == null) {
            assertThat(requestMap, aMapWithSize(2));
        } else {
            assertThat(requestMap, aMapWithSize(3));
        }
        assertThat(requestMap.get("input"), is(List.of("ABCD")));
        assertThat(requestMap.get("model"), is("llama-embed"));
        assertThat(requestMap.get("user"), is(user));
        assertNull(httpPost.getFirstHeader("Authorization"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        testTruncate_ReducesInputTextSizeByHalf("user");
    }

    public void testTruncate_ReducesInputTextSizeByHalf_NoUser() throws IOException {
        testTruncate_ReducesInputTextSizeByHalf(null);
    }

    private static void testTruncate_ReducesInputTextSizeByHalf(String user) throws IOException {
        var request = createRequest(user, null, false);
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        if (user == null) {
            assertThat(requestMap, aMapWithSize(2));
        } else {
            assertThat(requestMap, aMapWithSize(3));
        }
        assertThat(requestMap.get("input"), is(List.of("AB")));
        assertThat(requestMap.get("user"), is(user));
        assertThat(requestMap.get("model"), is("llama-embed"));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest("user", null, false);
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

    private static LlamaEmbeddingsRequest createRequest(String user, Integer dimensions, boolean dimensionsSetByUser) {
        var embeddingsModel = LlamaEmbeddingsModelTests.createEmbeddingsModel(
            "llama-embed",
            "url",
            "apikey",
            user,
            dimensions,
            dimensionsSetByUser
        );
        return new LlamaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel
        );
    }

    private static LlamaEmbeddingsRequest createRequestNoAuth(String user) {
        var embeddingsModel = LlamaEmbeddingsModelTests.createEmbeddingsModelNoAuth("llama-embed", "url", user);
        return new LlamaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel
        );
    }

}
