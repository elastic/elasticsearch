/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.googlevertexai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModel;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankModelTests;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class GoogleVertexAiRerankRequestTests extends ESTestCase {

    private static final String AUTH_HEADER_VALUE = "foo";

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        var input = "input";
        var query = "query";

        var request = createRequest(query, input, null, null, null, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(2));
        assertThat(requestMap.get("records"), is(List.of(Map.of("id", "0", "content", input))));
        assertThat(requestMap.get("query"), is(query));
    }

    public void testCreateRequest_WithTopNSet() throws IOException {
        var input = "input";
        var query = "query";
        var topN = 1;
        var taskSettingsTopN = 3;

        var request = createRequest(query, input, null, topN, null, taskSettingsTopN);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("records"), is(List.of(Map.of("id", "0", "content", input))));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("topN"), is(topN));
    }

    public void testCreateRequest_UsesTaskSettingsTopNWhenRootLevelIsNull() throws IOException {
        var input = "input";
        var query = "query";
        var topN = 1;

        var request = createRequest(query, input, null, null, null, topN);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("records"), is(List.of(Map.of("id", "0", "content", input))));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("topN"), is(topN));
    }

    public void testCreateRequest_WithReturnDocumentsSet() throws IOException {
        var input = "input";
        var query = "query";

        var request = createRequest(query, input, null, null, Boolean.TRUE, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("records"), is(List.of(Map.of("id", "0", "content", input))));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("ignoreRecordDetailsInResponse"), is(Boolean.FALSE));
    }

    public void testCreateRequest_WithModelSet() throws IOException {
        var input = "input";
        var query = "query";
        var modelId = "model";

        var request = createRequest(query, input, modelId, null, null, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("records"), is(List.of(Map.of("id", "0", "content", input))));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("model"), is(modelId));
    }

    public void testTruncate_DoesNotTruncate() {
        var request = createRequest("query", "input", null, null, null, null);
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, sameInstance(request));
    }

    private static GoogleVertexAiRerankRequest createRequest(
        String query,
        String input,
        @Nullable String modelId,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable Integer taskSettingsTopN
    ) {
        var rerankModel = GoogleVertexAiRerankModelTests.createModel(modelId, taskSettingsTopN);

        return new GoogleVertexAiRerankWithoutAuthRequest(query, List.of(input), rerankModel, topN, returnDocuments);
    }

    /**
     * We use this class to fake the auth implementation to avoid static mocking of {@link GoogleVertexAiRequest}
     */
    private static class GoogleVertexAiRerankWithoutAuthRequest extends GoogleVertexAiRerankRequest {
        GoogleVertexAiRerankWithoutAuthRequest(
            String query,
            List<String> input,
            GoogleVertexAiRerankModel model,
            @Nullable Integer topN,
            @Nullable Boolean returnDocuments
        ) {
            super(query, input, returnDocuments, topN, model);
        }

        @Override
        public void decorateWithAuth(HttpPost httpPost) {
            httpPost.setHeader(HttpHeaders.AUTHORIZATION, AUTH_HEADER_VALUE);
        }
    }
}
