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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HuggingFaceInferenceRerankRequestTests extends ESTestCase {
    private static final String REQUEST_DOCUMENTS_FIELD = "documents";
    private static final String QUERY_FIELD = "query";

    @SuppressWarnings("unchecked")
    public void testCreateRequest() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abc", "ef");
        var httpRequest = huggingFaceRequest.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("www.google.com"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.size(), is(2));
        assertThat(requestMap.get(REQUEST_DOCUMENTS_FIELD), instanceOf(List.class));
        assertThat(requestMap.get(QUERY_FIELD), instanceOf(String.class));
        var documentsList = (List<String>) requestMap.get(REQUEST_DOCUMENTS_FIELD);
        var queryString = (String) requestMap.get(QUERY_FIELD);
        assertThat(documentsList, contains("abc"));
        assertThat(queryString, is("ef"));
    }

    public void testTruncate_ReducesInputTextSizeByHalf() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abcd", "ef");
        var truncatedRequest = huggingFaceRequest.truncate();
        assertThat(truncatedRequest.getURI().toString(), is(new URI("www.google.com").toString()));

        var httpRequest = truncatedRequest.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get(REQUEST_DOCUMENTS_FIELD), instanceOf(List.class));
        assertThat(requestMap.get(REQUEST_DOCUMENTS_FIELD), is(List.of("ab")));
    }

    public void testIsTruncated_ReturnsTrue() throws URISyntaxException, IOException {
        var huggingFaceRequest = createRequest("www.google.com", "secret", "abcd", "ef");
        assertFalse(huggingFaceRequest.getTruncationInfo()[0]);

        var truncatedRequest = huggingFaceRequest.truncate();
        assertTrue(truncatedRequest.getTruncationInfo()[0]);
    }

    public static HuggingFaceInferenceRerankRequest createRequest(String url, String apiKey,
                                                            String documents, String query) throws URISyntaxException {
        return new HuggingFaceInferenceRerankRequest(
            TruncatorTests.createTruncator(),
            new Truncator.TruncationResult(List.of(documents), new boolean[] { false }),
            HuggingFaceEmbeddingsModelTests.createModel(url, apiKey)
        );
    }
}
