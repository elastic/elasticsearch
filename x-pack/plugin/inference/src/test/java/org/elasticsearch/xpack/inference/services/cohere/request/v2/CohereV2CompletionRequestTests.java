/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.cohere.request.v2;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.cohere.completion.CohereCompletionModelTests;
import org.elasticsearch.xpack.inference.services.cohere.request.CohereUtils;
import org.hamcrest.CoreMatchers;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.is;

public class CohereV2CompletionRequestTests extends ESTestCase {

    public void testCreateRequest() throws IOException, URISyntaxException {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel(null, "secret", "required model id"),
            false
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();

        assertThat(httpPost.getUri().toString(), is("https://api.cohere.ai/v2/chat"));
        assertThat(httpPost.getBody().getContentType().toString(), is("application/json; charset=UTF-8"));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer secret"));
        assertThat(httpPost.getLastHeader(CohereUtils.REQUEST_SOURCE_HEADER).getValue(), is(CohereUtils.ELASTIC_REQUEST_SOURCE));

        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(
            requestMap,
            is(Map.of("messages", List.of(Map.of("role", "user", "content", "abc")), "model", "required model id", "stream", false))
        );
    }

    public void testDefaultUrl() throws URISyntaxException {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel(null, "secret", "model id"),
            false
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = httpRequest.httpRequest();

        assertThat(httpPost.getUri().toString(), is("https://api.cohere.ai/v2/chat"));
    }

    public void testOverriddenUrl() throws URISyntaxException {
        var request = new CohereV2CompletionRequest(
            List.of("abc"),
            CohereCompletionModelTests.createModel("http://localhost", "secret", "model id"),
            false
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);
        var httpPost = httpRequest.httpRequest();

        assertThat(httpPost.getUri().toString(), is("http://localhost/v2/chat"));
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
            {"messages":[{"role":"user","content":"some input"}],"model":"model","stream":false}"""));
    }
}
