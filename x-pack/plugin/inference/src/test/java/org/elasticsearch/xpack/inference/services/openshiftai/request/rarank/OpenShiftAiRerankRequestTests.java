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

    private static final String TOP_N_FIELD_NAME = "top_n";
    private static final String RETURN_DOCUMENTS_FIELD_NAME = "return_documents";
    private static final String MODEL_FIELD_NAME = "model";
    private static final String DOCUMENTS_FIELD_NAME = "documents";
    private static final String QUERY_FIELD_NAME = "query";

    private static final String DOCUMENT_VALUE = "some document";
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_VALUE = "some model";
    private static final Integer TOP_N_VALUE = 8;
    private static final Boolean RETURN_DOCUMENTS_VALUE = false;
    private static final String API_KEY_VALUE = "some api key";

    public void testCreateRequest_WithMinimalFieldsSet() throws IOException {
        testCreateRequest(null, null, null, createRequest(null, null, null));
    }

    public void testCreateRequest_WithTopN() throws IOException {
        testCreateRequest(TOP_N_VALUE, null, null, createRequest(TOP_N_VALUE, null, null));
    }

    public void testCreateRequest_WithReturnDocuments() throws IOException {
        testCreateRequest(null, RETURN_DOCUMENTS_VALUE, null, createRequest(null, RETURN_DOCUMENTS_VALUE, null));
    }

    public void testCreateRequest_WithModelId() throws IOException {
        testCreateRequest(null, null, MODEL_VALUE, createRequest(null, null, MODEL_VALUE));
    }

    public void testCreateRequest_AllFields() throws IOException {
        testCreateRequest(
            TOP_N_VALUE,
            RETURN_DOCUMENTS_VALUE,
            MODEL_VALUE,
            createRequest(TOP_N_VALUE, RETURN_DOCUMENTS_VALUE, MODEL_VALUE)
        );
    }

    public void testCreateRequest_AllFields_OverridesTaskSettings() throws IOException {
        testCreateRequest(
            TOP_N_VALUE,
            RETURN_DOCUMENTS_VALUE,
            MODEL_VALUE,
            createRequestWithDifferentTaskSettings(TOP_N_VALUE, RETURN_DOCUMENTS_VALUE)
        );
    }

    public void testCreateRequest_AllFields_KeepsTaskSettings() throws IOException {
        testCreateRequest(1, true, MODEL_VALUE, createRequestWithDifferentTaskSettings(null, null));
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
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is("Bearer %s".formatted(API_KEY_VALUE)));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(DOCUMENTS_FIELD_NAME), is(List.of(DOCUMENT_VALUE)));
        assertThat(requestMap.get(QUERY_FIELD_NAME), is(QUERY_VALUE));
        int itemsCount = 2;
        if (expectedTopN != null) {
            assertThat(requestMap.get(TOP_N_FIELD_NAME), is(expectedTopN));
            itemsCount++;
        }
        if (expectedReturnDocuments != null) {
            assertThat(requestMap.get(RETURN_DOCUMENTS_FIELD_NAME), is(expectedReturnDocuments));
            itemsCount++;
        }
        if (expectedModelId != null) {
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(expectedModelId));
            itemsCount++;
        }
        assertThat(requestMap, aMapWithSize(itemsCount));
    }

    private static OpenShiftAiRerankRequest createRequest(
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments,
        @Nullable String modelId
    ) {
        var rerankModel = OpenShiftAiRerankModelTests.createModel(randomAlphaOfLength(10), API_KEY_VALUE, modelId, topN, returnDocuments);
        return new OpenShiftAiRerankRequest(QUERY_VALUE, List.of(DOCUMENT_VALUE), returnDocuments, topN, rerankModel);
    }

    private static OpenShiftAiRerankRequest createRequestWithDifferentTaskSettings(
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        var rerankModel = OpenShiftAiRerankModelTests.createModel(randomAlphaOfLength(10), API_KEY_VALUE, MODEL_VALUE, 1, true);
        return new OpenShiftAiRerankRequest(QUERY_VALUE, List.of(DOCUMENT_VALUE), returnDocuments, topN, rerankModel);
    }
}
