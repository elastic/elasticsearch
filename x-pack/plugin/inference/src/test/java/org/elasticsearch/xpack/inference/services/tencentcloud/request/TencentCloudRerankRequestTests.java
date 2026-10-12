/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.rerank.TencentCloudRerankTaskSettings;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class TencentCloudRerankRequestTests extends ESTestCase {

    private static final String MODEL_ID = "bge-reranker-v2-m3";
    private static final String API_KEY = "sk-12345";
    private static final String QUERY = "some query";
    private static final String DOCUMENT = "some document";
    private static final String DEFAULT_REGION = "bj";
    private static final String CUSTOM_REGION = "sh";
    private static final String DEFAULT_URI = "https://bj.aisearch.tencentelasticsearch.com/v1/rerank";
    private static final String CUSTOM_REGION_URI = "https://sh.aisearch.tencentelasticsearch.com/v1/rerank";

    public void testCreateRequest_DefaultRegion_SetsCorrectUri() {
        var request = createRequest(DEFAULT_REGION, null, null);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(DEFAULT_URI));
    }

    public void testCreateRequest_CustomRegion_SetsCorrectUri() {
        var request = createRequest(CUSTOM_REGION, null, null);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(CUSTOM_REGION_URI));
    }

    public void testCreateRequest_SetsAuthorizationHeader() {
        var request = createRequest(DEFAULT_REGION, null, null);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY)));
    }

    public void testCreateRequest_SetsContentTypeHeader() {
        var request = createRequest(DEFAULT_REGION, null, null);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), containsString("application/json"));
    }

    public void testCreateRequest_MinimalBody() throws IOException {
        var request = createRequest(DEFAULT_REGION, null, null);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap.get("query"), is(QUERY));
        assertThat(requestMap.get("documents"), is(List.of(DOCUMENT)));
        assertThat(requestMap, aMapWithSize(3));
    }

    public void testCreateRequest_WithTopNAndReturnDocuments() throws IOException {
        var request = createRequest(DEFAULT_REGION, 3, true);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("top_n"), is(3));
        assertThat(requestMap.get("return_documents"), is(true));
        assertThat(requestMap, aMapWithSize(5));
    }

    private static TencentCloudRerankRequest createRequest(String region, Integer topN, Boolean returnDocuments) {
        var serviceSettings = new TencentCloudRerankServiceSettings(MODEL_ID, region, new RateLimitSettings(20));
        var model = new TencentCloudRerankModel(
            "test-inference-id",
            serviceSettings,
            TencentCloudRerankTaskSettings.EMPTY_SETTINGS,
            new DefaultSecretSettings(new SecureString(API_KEY.toCharArray()))
        );
        return new TencentCloudRerankRequest(QUERY, List.of(DOCUMENT), returnDocuments, topN, model);
    }
}
