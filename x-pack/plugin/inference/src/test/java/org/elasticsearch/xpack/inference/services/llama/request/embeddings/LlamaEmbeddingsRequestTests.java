/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.request.embeddings;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModelTests;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

public class LlamaEmbeddingsRequestTests extends ESTestCase {

    public void testCreateRequest_WithAuth_Success() throws IOException, URISyntaxException {
        var request = createRequest();
        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("contents"), is(List.of("ABCD")));
        assertThat(requestMap.get("model_id"), is("llama-embed"));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));
    }

    public void testCreateRequest_NoAuth_Success() throws IOException, URISyntaxException {
        var request = createRequestNoAuth();
        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = validateRequestUrlAndContentType(httpRequest);

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("contents"), is(List.of("ABCD")));
        assertThat(requestMap.get("model_id"), is("llama-embed"));
        assertNull(httpPost.getFirstHeader("Authorization"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest();
        var truncatedRequest = request.truncate();

        var httpRequest = RequestTests.getHttpRequestSync(truncatedRequest);

        var httpPost = httpRequest.httpRequest();
        var requestMap = entityAsMap(httpPost.getBodyText());
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

    private SimpleHttpRequest validateRequestUrlAndContentType(HttpRequest request) throws URISyntaxException {
        var httpPost = request.httpRequest();
        assertThat(httpPost.getUri().toString(), is("url"));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
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
