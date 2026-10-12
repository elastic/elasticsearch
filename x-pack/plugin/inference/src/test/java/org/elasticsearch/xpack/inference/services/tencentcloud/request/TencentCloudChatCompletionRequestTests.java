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
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.external.http.sender.UnifiedChatInput;
import org.elasticsearch.xpack.inference.external.request.RequestTests;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionModel;
import org.elasticsearch.xpack.inference.services.tencentcloud.completion.TencentCloudChatCompletionServiceSettings;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TencentCloudChatCompletionRequestTests extends ESTestCase {

    private static final String MODEL_ID = "deepseek-v3";
    private static final String API_KEY = "sk-12345";
    private static final String ROLE = "user";
    private static final String DEFAULT_REGION = "bj";
    private static final String CUSTOM_REGION = "sh";
    private static final String DEFAULT_URI = "https://bj.aisearch.tencentelasticsearch.com/v1/chat/completions";
    private static final String CUSTOM_REGION_URI = "https://sh.aisearch.tencentelasticsearch.com/v1/chat/completions";

    public void testCreateRequest_DefaultRegion_SetsCorrectUri() {
        var request = createRequest(DEFAULT_REGION, randomAlphaOfLength(10), false);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(DEFAULT_URI));
    }

    public void testCreateRequest_CustomRegion_SetsCorrectUri() {
        var request = createRequest(CUSTOM_REGION, randomAlphaOfLength(10), false);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        assertThat(httpRequest.httpRequestBase(), instanceOf(HttpPost.class));
        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getURI().toString(), is(CUSTOM_REGION_URI));
    }

    public void testCreateRequest_SetsAuthorizationHeader() {
        var request = createRequest(DEFAULT_REGION, randomAlphaOfLength(10), false);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", API_KEY)));
    }

    public void testCreateRequest_SetsContentTypeHeader() {
        var request = createRequest(DEFAULT_REGION, randomAlphaOfLength(10), false);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        assertThat(httpPost.getLastHeader(HttpHeaders.CONTENT_TYPE).getValue(), containsString("application/json"));
    }

    public void testCreateRequest_Streaming_SetsStreamTrue() throws IOException {
        var input = randomAlphaOfLength(10);
        var request = createRequest(DEFAULT_REGION, input, true);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(true));
        assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap.get("n"), is(1));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", ROLE, "content", input))));
        assertThat(requestMap.get("stream_options"), is(Map.of("include_usage", true)));
        assertThat(requestMap, aMapWithSize(5));
    }

    public void testCreateRequest_NonStreaming_SetsStreamFalse() throws IOException {
        var input = randomAlphaOfLength(10);
        var request = createRequest(DEFAULT_REGION, input, false);
        var httpRequest = RequestTests.getHttpRequestSync(request);

        var httpPost = (HttpPost) httpRequest.httpRequestBase();
        var requestMap = entityAsMap(httpPost.getEntity().getContent());
        assertThat(requestMap.get("stream"), is(false));
        assertThat(requestMap.get("model"), is(MODEL_ID));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", ROLE, "content", input))));
    }

    public void testIsStreaming_ReturnsCorrectValue() {
        assertThat(createRequest(DEFAULT_REGION, "text", true).isStreaming(), is(true));
        assertThat(createRequest(DEFAULT_REGION, "text", false).isStreaming(), is(false));
    }

    public void testTruncate_ReturnsSameInstance() {
        var request = createRequest(DEFAULT_REGION, randomAlphaOfLength(5), false);
        assertThat(request.truncate(), sameInstance(request));
    }

    public void testGetTruncationInfo_ReturnsNull() {
        var request = createRequest(DEFAULT_REGION, randomAlphaOfLength(5), false);
        assertThat(request.getTruncationInfo(), is(nullValue()));
    }

    private static TencentCloudChatCompletionRequest createRequest(String region, String input, boolean stream) {
        var serviceSettings = new TencentCloudChatCompletionServiceSettings(MODEL_ID, region, new RateLimitSettings(5));
        var model = new TencentCloudChatCompletionModel(
            "test-inference-id",
            TaskType.CHAT_COMPLETION,
            serviceSettings,
            new DefaultSecretSettings(new SecureString(API_KEY.toCharArray()))
        );
        return new TencentCloudChatCompletionRequest(new UnifiedChatInput(List.of(input), ROLE, stream), model);
    }
}
