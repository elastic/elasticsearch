/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.contextualai.ContextualAiServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankModel;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.contextualai.rerank.ContextualAiRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.DEFAULT_RERANK_URL;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.INITIAL_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.NEW_TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_API_KEY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_DOCUMENTS;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INFERENCE_ENTITY_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_INSTRUCTION;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_MODEL_ID;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_QUERY;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_RATE_LIMIT;
import static org.elasticsearch.xpack.inference.services.contextualai.ContextualAiRerankTestFixtures.TEST_TOP_N;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.QUERY_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.TOP_N_FIELD;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankRequestTests extends ESTestCase {

    public void testCreateRequest_WithRequiredFieldsOnly() throws IOException {
        var requestMap = assertCreateHttpRequest(createRequest(ContextualAiRerankTaskSettings.EMPTY_SETTINGS, null));
        // Verifying that only query, documents, and model fields are present in the request
        assertThat(requestMap, aMapWithSize(3));
    }

    public void testCreateRequest_WithTopNAndInstructionFromTaskSettings() throws IOException {
        var requestMap = assertCreateHttpRequest(
            createRequest(new ContextualAiRerankTaskSettings(null, TEST_TOP_N, TEST_INSTRUCTION), null)
        );
        assertThat(requestMap.get(TOP_N_FIELD), is(TEST_TOP_N));
        assertThat(requestMap.get(INSTRUCTION_FIELD), is(TEST_INSTRUCTION));
        // Verifying that only query, documents, model, topN and instruction fields are present in the request
        assertThat(requestMap, aMapWithSize(5));
    }

    public void testCreateRequest_TopNFromRequestOverridesTopNFromTaskSettings() throws IOException {
        var requestMap = assertCreateHttpRequest(
            createRequest(new ContextualAiRerankTaskSettings(null, INITIAL_TEST_TOP_N, null), NEW_TEST_TOP_N)
        );
        assertThat(requestMap.get(TOP_N_FIELD), is(NEW_TEST_TOP_N));
        // Verifying that only query, documents, model, topN fields are present in the request
        assertThat(requestMap, aMapWithSize(4));
    }

    private static Map<String, Object> assertCreateHttpRequest(ContextualAiRerankRequest request) throws IOException {
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(DEFAULT_RERANK_URL));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", TEST_API_KEY)));

        assertThat(httpRequest.inferenceEntityId(), is(TEST_INFERENCE_ENTITY_ID));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(QUERY_FIELD), is(TEST_QUERY));
        assertThat(requestMap.get(DOCUMENTS_FIELD), is(TEST_DOCUMENTS));
        assertThat(requestMap.get(MODEL_FIELD), is(TEST_MODEL_ID));
        return requestMap;
    }

    private static ContextualAiRerankRequest createRequest(ContextualAiRerankTaskSettings taskSettings, Integer requestTopN) {
        var model = new ContextualAiRerankModel(
            TEST_INFERENCE_ENTITY_ID,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(TEST_MODEL_ID, new RateLimitSettings(TEST_RATE_LIMIT))
            ),
            taskSettings,
            new DefaultSecretSettings(new SecureString(TEST_API_KEY.toCharArray()))
        );
        return new ContextualAiRerankRequest(TEST_QUERY, TEST_DOCUMENTS, requestTopN, model);
    }
}
