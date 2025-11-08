/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.request.rarank;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModelTests;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class OpenShiftAiRerankRequestTests extends ESTestCase {
    private static final String INPUT = "documents";
    private static final String QUERY = "query";
    private static final String MODEL_ID = "modelId";
    private static final Integer TOP_N = 8;
    private static final Boolean RETURN_DOCUMENTS = false;
    private static final String API_KEY = "secret";

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        testCreateRequest(null, null, null, createRequest(null, null, null));
    }

    public void testCreateRequest_WithTopN() throws IOException {
        testCreateRequest(TOP_N, null, null, createRequest(TOP_N, null, null));
    }

    public void testCreateRequest_WithReturnDocuments() throws IOException {
        testCreateRequest(null, RETURN_DOCUMENTS, null, createRequest(null, RETURN_DOCUMENTS, null));
    }

    public void testCreateRequest_WithModelId() throws IOException {
        testCreateRequest(null, null, MODEL_ID, createRequest(null, null, MODEL_ID));
    }

    public void testCreateRequest_AllFields() throws IOException {
        testCreateRequest(TOP_N, RETURN_DOCUMENTS, MODEL_ID, createRequest(TOP_N, RETURN_DOCUMENTS, MODEL_ID));
    }

    public void testCreateRequest_AllFields_OverridesTaskSettings() throws IOException {
        testCreateRequest(TOP_N, RETURN_DOCUMENTS, MODEL_ID, createRequestWithDifferentTaskSettings(TOP_N, RETURN_DOCUMENTS));
    }

    public void testCreateRequest_AllFields_KeepsTaskSettings() throws IOException {
        testCreateRequest(1, true, MODEL_ID, createRequestWithDifferentTaskSettings(null, null));
    }

    private void testCreateRequest(
        Integer expectedTopN,
        Boolean expectedReturnDocuments,
        String expectedModelId,
        OpenShiftAiRerankRequest request
    ) throws IOException {
        var httpRequest = request.createHttpRequest();

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer %s".formatted(API_KEY)));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(INPUT), is(List.of(INPUT)));
        assertThat(requestMap.get(QUERY), is(QUERY));
        int itemsCount = 2;
        if (expectedTopN != null) {
            assertThat(requestMap.get("top_n"), is(expectedTopN));
            itemsCount++;
        }
        if (expectedReturnDocuments != null) {
            assertThat(requestMap.get("return_documents"), is(expectedReturnDocuments));
            itemsCount++;
        }
        if (expectedModelId != null) {
            assertThat(requestMap.get("model"), is(expectedModelId));
            itemsCount++;
        }
        assertThat(requestMap, aMapWithSize(itemsCount));
    }

    private static OpenShiftAiRerankRequest createRequest(
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable String modelId
    ) {
        var rerankModel = OpenShiftAiRerankModelTests.createModel(randomAlphaOfLength(10), API_KEY, modelId, topN, returnDocuments);
        return new OpenShiftAiRerankRequest(QUERY, List.of(INPUT), returnDocuments, topN, rerankModel);
    }

    private static OpenShiftAiRerankRequest createRequestWithDifferentTaskSettings(
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        var rerankModel = OpenShiftAiRerankModelTests.createModel(randomAlphaOfLength(10), API_KEY, MODEL_ID, 1, true);
        return new OpenShiftAiRerankRequest(QUERY, List.of(INPUT), returnDocuments, topN, rerankModel);
    }
}
