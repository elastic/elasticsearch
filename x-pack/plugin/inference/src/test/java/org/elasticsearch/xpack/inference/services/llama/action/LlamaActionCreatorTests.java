/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.llama.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.llama.completion.LlamaChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingModelTests;
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
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class LlamaActionCreatorTests extends ESTestCase {

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

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "embeddings": [
                        [
                            -0.0123,
                            0.123
                        ]
                    ]
                {
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createEmbeddingsFuture(sender, createWithEmptySettings(threadPool));

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(TextEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { -0.0123F, 0.123F }))));

            assertEmbeddingsRequest();
        }
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction() throws IOException {
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                [
                    {
                        "embeddings": [
                            [
                                -0.0123,
                                0.123
                            ]
                        ]
                    {
                ]
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createEmbeddingsFuture(sender, createWithEmptySettings(threadPool));

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to parse object: expecting token of type [START_ARRAY] but found [START_OBJECT]")
            );

            assertEmbeddingsRequest();
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForCompletionAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "id": "chatcmpl-03e70a75-efb6-447d-b661-e5ed0bd59ce9",
                    "choices": [
                        {
                            "finish_reason": "length",
                            "index": 0,
                            "logprobs": null,
                            "message": {
                                "content": "Hello there, how may I assist you today?",
                                "refusal": null,
                                "role": "assistant",
                                "annotations": null,
                                "audio": null,
                                "function_call": null,
                                "tool_calls": null
                            }
                        }
                    ],
                    "created": 1750157476,
                    "model": "llama3.2:3b",
                    "object": "chat.completion",
                    "service_tier": null,
                    "system_fingerprint": "fp_ollama",
                    "usage": {
                        "completion_tokens": 10,
                        "prompt_tokens": 30,
                        "total_tokens": 40,
                        "completion_tokens_details": null,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createCompletionFuture(sender, createWithEmptySettings(threadPool));

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there, how may I assist you today?"))));

            assertCompletionRequest();
        }
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForCompletionAction() throws IOException {
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
                    "invalid_field": "unexpected"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createCompletionFuture(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Llama completion request from inference entity id [id]. Cause: Required [choices]")
            );

            assertCompletionRequest();
        }
    }

    private PlainActionFuture<InferenceServiceResults> createEmbeddingsFuture(Sender sender, ServiceComponents threadPool) {
        var model = LlamaEmbeddingModelTests.createEmbeddingsModel("model", getUrl(webServer), "secret");
        var actionCreator = new LlamaActionCreator(sender, threadPool);
        var action = actionCreator.create(model);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );
        return listener;
    }

    private PlainActionFuture<InferenceServiceResults> createCompletionFuture(Sender sender, ServiceComponents threadPool) {
        var model = LlamaChatCompletionModelTests.createCompletionModel("model", getUrl(webServer), "secret");
        var actionCreator = new LlamaActionCreator(sender, threadPool);
        var action = actionCreator.create(model);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("Hello"), false), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
        return listener;
    }

    private void assertCompletionRequest() throws IOException {
        assertCommonRequestProperties();

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        assertThat(requestMap.size(), is(4));
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "Hello"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertThat(requestMap.get("stream"), is(false));
    }

    @SuppressWarnings("unchecked")
    private void assertEmbeddingsRequest() throws IOException {
        assertCommonRequestProperties();

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        assertThat(requestMap.size(), is(2));
        assertThat(requestMap.get("contents"), instanceOf(List.class));
        var inputList = (List<String>) requestMap.get("contents");
        assertThat(inputList, contains("abc"));
    }

    private void assertCommonRequestProperties() {
        assertThat(webServer.requests(), hasSize(1));
        assertNull(webServer.requests().get(0).getUri().getQuery());
        assertThat(
            webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
            equalTo(XContentType.JSON.mediaTypeWithoutParameters())
        );
        assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
    }
}
