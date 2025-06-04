/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionTaskSettingsTests;
import org.elasticsearch.xpack.inference.services.anthropic.request.AnthropicRequestUtils;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AnthropicActionCreatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreate_ChatCompletionModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-3-opus-20240229",
                    "content": [
                        {
                            "type": "text",
                            "text": "San Francisco has a cool-summer Mediterranean climate."
                        }
                    ],
                    "stop_reason": "end_turn",
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": 16,
                        "output_tokens": 326
                    }
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = AnthropicChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "secret", "model", 0);
            var actionCreator = new AnthropicActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, 2.0, -3.0, 3);
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("San Francisco has a cool-summer Mediterranean climate."))));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);

            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(request.getHeader(AnthropicRequestUtils.X_API_KEY), equalTo("secret"));
            assertThat(request.getHeader(AnthropicRequestUtils.VERSION), equalTo(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(6));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("max_tokens"), is(1));
            assertThat(requestMap.get("temperature"), is(2.0));
            assertThat(requestMap.get("top_p"), is(-3.0));
            assertThat(requestMap.get("top_k"), is(3));
        }
    }

    public void testCreate_ChatCompletionModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "id": "msg_01XzZQmG41BMGe5NZ5p2vEWb",
                    "type": "message",
                    "role": "assistant",
                    "model": "claude-3-opus-20240229",
                    "content_does_not_exist": [
                        {
                            "type": "text",
                            "text": "San Francisco has a cool-summer Mediterranean climate."
                        }
                    ],
                    "stop_reason": "end_turn",
                    "stop_sequence": null,
                    "usage": {
                        "input_tokens": 16,
                        "output_tokens": 326
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = AnthropicChatCompletionModelTests.createChatCompletionModel(getUrl(webServer), "secret", "model", 0);
            var actionCreator = new AnthropicActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = AnthropicChatCompletionTaskSettingsTests.getChatCompletionTaskSettingsMap(1, null, null, null);
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var failureCauseMessage = "Failed to find required field [content] in Anthropic chat completions response";
            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Anthropic chat completions request. Cause: " + failureCauseMessage)
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(AnthropicRequestUtils.X_API_KEY), equalTo("secret"));
            assertThat(
                webServer.requests().get(0).getHeader(AnthropicRequestUtils.VERSION),
                equalTo(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01)
            );

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("max_tokens"), is(1));
        }
    }
}
