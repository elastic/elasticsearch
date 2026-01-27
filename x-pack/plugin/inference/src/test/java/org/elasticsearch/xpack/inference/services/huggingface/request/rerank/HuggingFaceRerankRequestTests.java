/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.huggingface.request.rerank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModel;
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class HuggingFaceRerankRequestTests extends ESTestCase {
    private static final String INPUT = "texts";
    private static final String QUERY = "query";
    private static final String INFERENCE_ID = "model";
    private static final Integer TOP_N = 8;
    private static final Boolean RETURN_TEXT = false;

    private static final String AUTH_HEADER_VALUE = "foo";

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        testCreateRequest(null, null);
    }

    public void testCreateRequest_WithTopN() throws IOException {
        testCreateRequest(TOP_N, null);
    }

    public void testCreateRequest_WithReturnDocuments() throws IOException {
        testCreateRequest(null, RETURN_TEXT);
    }

    private void testCreateRequest(Integer topN, Boolean returnDocuments) throws IOException {
        var request = createRequest(topN, returnDocuments);
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(AUTH_HEADER_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(INPUT), is(List.of(INPUT)));
        assertThat(requestMap.get(QUERY), is(QUERY));
        // input and query must exist
        int itemsCount = 2;
        if (topN != null) {
            assertThat(requestMap.get("top_n"), is(topN));
            itemsCount++;
        }
        if (returnDocuments != null) {
            assertThat(requestMap.get("return_text"), is(returnDocuments));
            itemsCount++;
        }
        assertThat(requestMap, aMapWithSize(itemsCount));
    }

    private static HuggingFaceRerankRequest createRequest(@Nullable Integer topN, @Nullable Boolean returnDocuments) {
        var rerankModel = HuggingFaceRerankModelTests.createModel(randomAlphaOfLength(10), "secret", INFERENCE_ID, topN, returnDocuments);

        return new HuggingFaceRerankWithoutAuthRequest(QUERY, List.of(INPUT), rerankModel, topN, returnDocuments);
    }

    /**
     * We use this class to fake the auth implementation to avoid static mocking of {@link HuggingFaceRerankRequest}
     */
    private static class HuggingFaceRerankWithoutAuthRequest extends HuggingFaceRerankRequest {
        HuggingFaceRerankWithoutAuthRequest(
            String query,
            List<String> input,
            HuggingFaceRerankModel model,
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
