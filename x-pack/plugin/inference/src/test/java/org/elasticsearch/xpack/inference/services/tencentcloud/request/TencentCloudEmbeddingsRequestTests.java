/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.tencentcloud.request;

import org.apache.hc.core5.http.HttpHeaders;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.inference.SimilarityMeasure;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.embeddings.TencentCloudEmbeddingsTaskSettings;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class TencentCloudEmbeddingsRequestTests extends ESTestCase {

    private static final String MODEL_ID = "bge-m3";
    private static final String API_KEY = "sk-12345";
    private static final String INPUT_VALUE = "some text";
    private static final String DEFAULT_REGION = "bj";
    private static final String CUSTOM_REGION = "sh";
    private static final String DEFAULT_URI = "https://bj.aisearch.tencentelasticsearch.com/v1/embeddings";
    private static final String CUSTOM_REGION_URI = "https://sh.aisearch.tencentelasticsearch.com/v1/embeddings";

    public void testCreateRequest_DefaultRegion_SetsCorrectUri() throws URISyntaxException {
        var request = createRequest(DEFAULT_REGION);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getUri().toString(), is(DEFAULT_URI));
    }

    public void testCreateRequest_CustomRegion_SetsCorrectUri() throws URISyntaxException {
        var request = createRequest(CUSTOM_REGION);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getUri().toString(), is(CUSTOM_REGION_URI));
    }

    public void testCreateRequest_SetsAuthorizationHeader() {
        var request = createRequest(DEFAULT_REGION);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY)));
    }

    public void testCreateRequest_SetsContentTypeHeader() {
        var request = createRequest(DEFAULT_REGION);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), containsString("application/json"));
    }

    public void testCreateRequest_SetsCorrectBody() throws IOException {
        var request = createRequest(DEFAULT_REGION);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = httpRequest.httpRequest();
        var requestMap = entityAsMap(httpPost.getBodyText());
        assertThat(requestMap.get("input"), is(List.of(INPUT_VALUE)));
        assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap, aMapWithSize(2));
    }

    private static TencentCloudEmbeddingsRequest createRequest(String region) {
        var serviceSettings = new TencentCloudEmbeddingsServiceSettings(
            MODEL_ID,
            region,
            new RateLimitSettings(20),
            SimilarityMeasure.COSINE,
            null,
            null
        );
        var model = new TencentCloudEmbeddingsModel(
            "test-inference-id",
            serviceSettings,
            TencentCloudEmbeddingsTaskSettings.EMPTY_SETTINGS,
            null,
            new DefaultSecretSettings(new SecureString(API_KEY.toCharArray()))
        );
        return new TencentCloudEmbeddingsRequest(List.of(INPUT_VALUE), model);
    }
}
