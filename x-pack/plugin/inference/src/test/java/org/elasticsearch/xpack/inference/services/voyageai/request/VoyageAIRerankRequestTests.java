/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

public class VoyageAIRerankRequestTests extends ESTestCase {

    private static final String API_KEY = "foo";

    public void testCreateRequest_WithMinimalFields() throws IOException {
        var input = "input";
        var query = "query";
        var modelId = "model";

        var request = createRequest(query, input, modelId, null, null, null);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + API_KEY));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("documents"), is(List.of(input)));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("model"), is(modelId));
    }

    public void testCreateRequest_WithAllFieldsDefined() throws IOException {
        var input = "input";
        var query = "query";
        var topK = 1;
        var taskSettingsTopK = 2;
        var modelId = "model";

        var request = createRequest(query, input, modelId, topK, Boolean.FALSE, taskSettingsTopK);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + API_KEY));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(5));
        assertThat(requestMap.get("documents"), is(List.of(input)));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("top_k"), is(topK));
        assertThat(requestMap.get("model"), is(modelId));
        assertThat(requestMap.get("return_documents"), is(Boolean.FALSE));
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
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer " + API_KEY));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get("documents"), is(List.of(input)));
        assertThat(requestMap.get("query"), is(query));
        assertThat(requestMap.get("model"), is(modelId));
    }

    public void testTruncate_DoesNotTruncate() {
        var request = createRequest("query", "input", "null", null, null, null);
        var truncatedRequest = request.truncate();

        assertThat(truncatedRequest, sameInstance(request));
    }

    private static VoyageAIRerankRequest createRequest(
        String query,
        String input,
        @Nullable String modelId,
        @Nullable Integer topK,
        @Nullable Boolean returnDocuments,
        @Nullable Integer taskSettingsTopK
    ) {
        var rerankModel = VoyageAIRerankModelTests.createModel(API_KEY, modelId, taskSettingsTopK);
        return new VoyageAIRerankRequest(query, List.of(input), returnDocuments, topK, rerankModel);

    }
}
