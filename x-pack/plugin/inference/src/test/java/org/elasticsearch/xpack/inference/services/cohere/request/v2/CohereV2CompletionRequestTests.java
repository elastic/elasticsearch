/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v2;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModelTests;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class CohereV2CompletionRequestTests extends ESTestCase {

    public void testCreateRequest() throws IOException {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel(null, "secret", "required model id"),
            false
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));

        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("https://api.cohere.ai/v2/chat"));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(), is(CohereUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap, is(Map.of("message", "abc", "model", "required model id", "stream", false)));
    }

    public void testDefaultUrl() {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel(null, "secret", "model id"),
            false
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("https://api.cohere.ai/v2/chat"));
    }

    public void testOverriddenUrl() {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel("http://localhost", "secret", "model id"),
            false
        );

        var httpRequest = request.createHttpRequest();
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is("http://localhost/v2/chat"));
    }

    public void testXContents() throws IOException {
        var request = new CohereV2CompletionRequest(
            List.of("some input"),
            CohereCompletionModelTests.createModel(null, "secret", "model"),
            false
        );
        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        request.toXContent(builder, null);
        String xContentResult = Strings.toString(builder);

        assertThat(xContentResult, CoreMatchers.is("""
            {"message":"some input","model":"model","stream":false}"""));
    }
}
