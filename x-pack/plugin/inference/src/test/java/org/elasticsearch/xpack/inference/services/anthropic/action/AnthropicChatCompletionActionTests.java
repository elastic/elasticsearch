/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.anthropic.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.inference.external.action.ExecutableAction;
import org.elasticsearch.xpack.inference.external.action.SingleInputSenderExecutableAction;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.anthropic.AnthropicCompletionRequestManager;
import org.elasticsearch.xpack.inference.services.anthropic.completion.AnthropicChatCompletionModelTests;
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
import static org.elasticsearch.xpack.inference.external.action.ActionUtils.constructFailedToSendRequestMessage;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class AnthropicChatCompletionActionTests extends ESTestCase {

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

    public void testExecute_ReturnsSuccessfulResponse() throws IOException {
        var senderFactory = new HttpRequestSender.Factory(createWithEmptySettings(threadPool), clientManager, mockClusterServiceEmpty());

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

            var action = createAction(getUrl(webServer), "secret", "model", 1, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("San Francisco has a cool-summer Mediterranean climate."))));
            assertThat(webServer.requests(), hasSize(1));

            MockRequest request = webServer.requests().get(0);

            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(request.getHeader(AnthropicRequestUtils.X_API_KEY), equalTo("secret"));
            assertThat(request.getHeader(AnthropicRequestUtils.VERSION), equalTo(AnthropicRequestUtils.ANTHROPIC_VERSION_2023_06_01));

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("max_tokens"), is(1));
        }
    }

    public void testExecute_ThrowsURISyntaxException_ForInvalidUrl() throws IOException {
        try (var sender = mock(Sender.class)) {
            var thrownException = expectThrows(IllegalArgumentException.class, () -> createAction("^^", "secret", "model", 1, sender));
            assertThat(thrownException.getMessage(), containsString("unable to parse url [^^]"));
        }
    }

    public void testExecute_ThrowsElasticsearchException() {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", 1, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("failed"));
    }

    public void testExecute_ThrowsElasticsearchException_WhenSenderOnFailureIsCalled() {
        var sender = mock(Sender.class);

        doAnswer(invocation -> {
            ActionListener<InferenceServiceResults> listener = invocation.getArgument(3);
            listener.onFailure(new IllegalStateException("failed"));

            return Void.TYPE;
        }).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", 1, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send Anthropic chat completions request. Cause: failed"));
    }

    public void testExecute_ThrowsException() {
        var sender = mock(Sender.class);
        doThrow(new IllegalArgumentException("failed")).when(sender).send(any(), any(), any(), any());

        var action = createAction(getUrl(webServer), "secret", "model", 1, sender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(thrownException.getMessage(), is("Failed to send Anthropic chat completions request. Cause: failed"));
    }

    public void testExecute_ThrowsException_WhenInputIsGreaterThanOne() throws IOException {
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

            var action = createAction(getUrl(webServer), "secret", "model", 1, sender);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc", "def")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(thrownException.getMessage(), is("Anthropic chat completions only accepts 1 input"));
            assertThat(thrownException.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    private ExecutableAction createAction(String url, String apiKey, String modelName, int maxTokens, Sender sender) {
        var model = AnthropicChatCompletionModelTests.createChatCompletionModel(url, apiKey, modelName, maxTokens);
        var requestCreator = AnthropicCompletionRequestManager.of(model, threadPool);
        var errorMessage = constructFailedToSendRequestMessage("Anthropic chat completions");
        return new SingleInputSenderExecutableAction(sender, requestCreator, errorMessage, "Anthropic chat completions");
    }
}
