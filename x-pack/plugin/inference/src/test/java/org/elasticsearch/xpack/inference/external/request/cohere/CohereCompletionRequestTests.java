/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request.cohere;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.cohere.completion.CohereCompletionRequest;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class CohereCompletionRequestTests extends ESTestCase {

    public void testCreateRequest_UrlDefined() throws IOException {
        var request = new CohereCompletionRequest(List.of("abc"), CohereCompletionModelTests.createModel("url", "secret", null), false);

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("url"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(), is(CohereUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(Map.of("message", "abc")));
    }

    public void testCreateRequest_ModelDefined() throws IOException {
        var request = new CohereCompletionRequest(List.of("abc"), CohereCompletionModelTests.createModel("url", "secret", "model"), false);

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("url"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(), is(CohereUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(Map.of("message", "abc", "model", "model")));
    }

    public void testTruncate_ReturnsSameInstance() {
        var request = new CohereCompletionRequest(List.of("abc"), CohereCompletionModelTests.createModel("url", "secret", "model"), false);
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, sameInstance(request));
    }

    public void testTruncationInfo_ReturnsNull() {
        var request = new CohereCompletionRequest(List.of("abc"), CohereCompletionModelTests.createModel("url", "secret", "model"), false);

        assertNull(request.getTruncationInfo());
    }
}
