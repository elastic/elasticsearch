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
import java.net.URI;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.DOCUMENTS_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.INSTRUCTION_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.MODEL_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.QUERY_FIELD;
import static org.elasticsearch.xpack.inference.services.contextualai.request.ContextualAiRerankRequestEntity.TOP_N_FIELD;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class ContextualAiRerankRequestTests extends ESTestCase {

    private static final String URL_VALUE = "http://www.abc.com";
    private static final String DOCUMENT_VALUE = "some document";
    private static final String QUERY_VALUE = "some query";
    private static final String MODEL_ID_VALUE = "some_model";
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String INFERENCE_ENTITY_ID_VALUE = "test-inference-entity-id";
    private static final int TOP_N_VALUE = 7;
    private static final String INSTRUCTION_VALUE = "Rerank by relevance.";

    public void testCreateRequest_CustomUrl() throws IOException {
        assertCreateHttpRequest(createRequest(URI.create(URL_VALUE), new ContextualAiRerankTaskSettings(null, null, null), null, null));
    }

    public void testCreateRequest_WithTopNAndInstruction() throws IOException {
        var request = createRequest(
            URI.create(URL_VALUE),
            new ContextualAiRerankTaskSettings(null, null, null),
            TOP_N_VALUE,
            INSTRUCTION_VALUE
        );

        var httpRequest = RequestTests.getHttpRequestSync(request);
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(URL_VALUE));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));
        assertThat(httpRequest.inferenceEntityId(), is(INFERENCE_ENTITY_ID_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get(QUERY_FIELD), is(QUERY_VALUE));
        assertThat(requestMap.get(DOCUMENTS_FIELD), is(List.of(DOCUMENT_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD), is(MODEL_ID_VALUE));
        assertThat(requestMap.get(TOP_N_FIELD), is(TOP_N_VALUE));
        assertThat(requestMap.get(INSTRUCTION_FIELD), is(INSTRUCTION_VALUE));
        assertThat(requestMap, aMapWithSize(5));
    }

    public void testCreateRequest_TopNFallbackFromModelTaskSettings() throws IOException {
        int topNFromTaskSettings = 4;
        var request = createRequest(
            URI.create(URL_VALUE),
            new ContextualAiRerankTaskSettings(null, topNFromTaskSettings, null),
            null,
            null
        );
        assertThat(request.getTopN(), is(topNFromTaskSettings));

        var httpRequest = RequestTests.getHttpRequestSync(request);
        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get(TOP_N_FIELD), is(topNFromTaskSettings));
        assertThat(requestMap, aMapWithSize(4));
    }

    public void testGetInferenceEntityId() {
        var request = createRequest(URI.create(URL_VALUE), new ContextualAiRerankTaskSettings(null, null, null), null, null);
        assertThat(request.getInferenceEntityId(), is(INFERENCE_ENTITY_ID_VALUE));
    }

    public void testGetURI() {
        var uri = URI.create(URL_VALUE);
        var request = createRequest(uri, new ContextualAiRerankTaskSettings(null, null, null), null, null);
        assertThat(request.getURI(), is(uri));
    }

    private void assertCreateHttpRequest(ContextualAiRerankRequest request) throws IOException {
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();

        assertThat(httpPost.getURI().toString(), is(URL_VALUE));
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(httpPost.getLastHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY_VALUE)));

        assertThat(httpRequest.inferenceEntityId(), is(INFERENCE_ENTITY_ID_VALUE));

        var requestMap = entityAsMap(httpPost.getEntity().getContent());

        assertThat(requestMap.get(QUERY_FIELD), is(QUERY_VALUE));
        assertThat(requestMap.get(DOCUMENTS_FIELD), is(List.of(DOCUMENT_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD), is(MODEL_ID_VALUE));
        assertThat(requestMap, aMapWithSize(3));
    }

    private static ContextualAiRerankRequest createRequest(
        URI serviceUri,
        ContextualAiRerankTaskSettings taskSettings,
        Integer requestTopN,
        String instruction
    ) {
        var model = new ContextualAiRerankModel(
            INFERENCE_ENTITY_ID_VALUE,
            new ContextualAiRerankServiceSettings(
                new ContextualAiServiceSettings.CommonSettings(serviceUri, MODEL_ID_VALUE, new RateLimitSettings(1000L))
            ),
            taskSettings,
            new DefaultSecretSettings(new SecureString(API_KEY_VALUE.toCharArray()))
        );
        return new ContextualAiRerankRequest(QUERY_VALUE, List.of(DOCUMENT_VALUE), requestTopN, instruction, model);
    }
}
