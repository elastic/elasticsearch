/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.common.Truncator;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.services.huggingface.embeddings.HuggingFaceEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.huggingface.request.embeddings.HuggingFaceEmbeddingsRequest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HuggingFaceEmbeddingsRequestTests extends ESTestCase {
    @SuppressWarnings("unchecked")
    public void testCreateRequest() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abc");
        var httpRequest = huggingFaceRequest.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("www.google.com"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), is(1));
        assertThat(requestMap.get("inputs"), instanceOf(List.class));
        var inputList = (List<String>) requestMap.get("inputs");
        assertThat(inputList, contains("abc"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abcd");
        var truncatedRequest = huggingFaceRequest.truncate();
        assertThat(truncatedRequest.getURI().toString(), is(new URI("www.google.com").toString()));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("inputs"), instanceOf(List.class));
        assertThat(requestMap.get("inputs"), is(List.of("ab")));
    }

    public void testIsTruncated_ReturnsTrue() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abcd");
        assertFalse(huggingFaceRequest.getTruncationInfo()[0]);

        var truncatedRequest = huggingFaceRequest.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public static HuggingFaceEmbeddingsRequest createRequest(String url, String apiKey, String input) throws URISyntaxException {

        return new HuggingFaceEmbeddingsRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(input), new boolean[] { false }),
            HuggingFaceEmbeddingsModelTests.createModel(url, apiKey)
        );
    }
}
