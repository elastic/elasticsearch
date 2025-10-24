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

    public void testCreateRequest_WithAuth_Success() throws IOException {
        var request = createRequest();
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("contents"), is(List.of("ABCD")));
        assertThat(requestMap.get("model_id"), is("llama-embed"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));
    }

    public void testCreateRequest_NoAuth_Success() throws IOException {
        var request = createRequestNoAuth();
        var httpRequest = request.createHttpRequest();
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("contents"), is(List.of("ABCD")));
        assertThat(requestMap.get("model_id"), is("llama-embed"));
        assertNull(httpPost.getFirstHeader("Authorization"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest();
        var truncatedRequest = request.truncate();

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("contents"), is(List.of("AB")));
        assertThat(requestMap.get("model_id"), is("llama-embed"));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest();
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

    private static LlamaEmbeddingsRequest createRequest() {
        var embeddingsModel = LlamaEmbeddingsModelTests.createEmbeddingsModel("llama-embed", "url", "apikey");
        return new LlamaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel
        );
    }

    private static LlamaEmbeddingsRequest createRequestNoAuth() {
        var embeddingsModel = LlamaEmbeddingsModelTests.createEmbeddingsModelNoAuth("llama-embed", "url");
        return new LlamaEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of("ABCD"), new boolean[] { false }),
            embeddingsModel
        );
    }

}
