/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.request;

import org.apache.hc.client5.http.async.methods.SimpleHttpRequest;
import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.request.HttpRequest;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingModelTests;
import org.elasticsearch.xpack.inference.services.mistral.request.embeddings.MistralEmbeddingsRequest;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.is;

public class MistralEmbeddingsRequestTests extends ESTestCase {
    public void testCreateRequest_Works() throws IOException, URISyntaxException {
        var request = createRequest("mistral-embed", "apikey", "abcd");
        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = validateRequestUrlAndContentType(httpRequest, MistralConstants.API_EMBEDDINGS_PATH);
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer apikey"));

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of("abcd")));
        assertThat(requestMap.get("model"), is("mistral-embed"));
        assertThat(requestMap.get("encoding_format"), is("float"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws IOException {
        var request = createRequest("mistral-embed", "apikey", "abcd");
        var truncatedRequest = request.truncate();

        var httpRequest = RequestTests.getHttpRequestSync(truncatedRequest);

        var httpPost = httpRequest.httpRequest();
        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("input"), is(List.of("ab")));
        assertThat(requestMap.get("model"), is("mistral-embed"));
        assertThat(requestMap.get("encoding_format"), is("float"));
    }

    public void testIsTruncated_ReturnsTrue() {
        var request = createRequest("mistral-embed", "apikey", "abcd");
        assertFalse(request.getTruncationInfo()[0]);

        var truncatedRequest = request.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    private SimpleHttpRequest validateRequestUrlAndContentType(HttpRequest request, String expectedUrl) throws URISyntaxException {
        var httpPost = request.httpRequest();
        assertThat(httpPost.getUri().toString(), is(expectedUrl));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        return httpPost;
    }

    public static MistralEmbeddingsRequest createRequest(String model, String apiKey, String input) {
        var embeddingsModel = MistralEmbeddingModelTests.createModel("id", model, apiKey, null, null, null, null);
        return new MistralEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            embeddingsModel
        );
    }
}
