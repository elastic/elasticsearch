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
import org.elasticsearch.xpack.inference.services.llama.embeddings.LlamaEmbeddingsModelTests;
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
import static org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsRequestTaskSettingsTests.createRequestTaskSettingsMap;
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

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_WithUser_WithDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction("overridden_user", 384, false, null);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_NoUser_WithDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction(null, 384, false, null);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_WithUser_NoDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction("overridden_user", null, false, null);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_NoUser_NoDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction(null, null, false, null);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_WithUser_WithDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction("overridden_user", 384, true, 384);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_NoUser_WithDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction(null, 384, true, 384);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_WithUser_NoDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction("overridden_user", null, true, null);
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction_NoUser_NoDimensions_DimensionsSetByUserTrue() throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction(null, null, true, null);
    }

    private void testExecute_ReturnsSuccessfulResponse_ForEmbeddingsAction(
        String user,
        Integer dimensions,
        boolean dimensionsSetByUser,
        Integer expectedDimensions
    ) throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "object": "list",
                    "data": [
                        {
                            "object": "embedding",
                            "embedding": [
                                -0.123,
                                0.123
                            ],
                            "index": 0
                        }
                    ],
                    "model": "all-MiniLM-L6-v2",
                    "usage": {
                        "prompt_tokens": 4,
                        "total_tokens": 4
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createEmbeddingsFuture(
                sender,
                createWithEmptySettings(threadPool),
                user,
                dimensions,
                dimensionsSetByUser
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(TextEmbeddingFloatResultsTests.buildExpectationFloat(List.of(new float[] { -0.123F, 0.123F }))));

            assertEmbeddingsRequest(user, expectedDimensions);
        }
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_WithUser_WithDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction("overridden_user", 384, false, null);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_NoUser_WithDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction(null, 384, false, null);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_WithUser_NoDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction("overridden_user", null, false, null);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_NoUser_NoDimensions_DimensionsSetByUserFalse()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction(null, null, false, null);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_WithUser_WithDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction("overridden_user", 384, true, 384);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_NoUser_WithDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction(null, 384, true, 384);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_WithUser_NoDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction("overridden_user", null, true, null);
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction_NoUser_NoDimensions_DimensionsSetByUserTrue()
        throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction(null, null, true, null);
    }

    private void testExecute_FailsFromInvalidResponseFormat_ForEmbeddingsAction(
        String user,
        Integer dimensions,
        boolean dimensionsSetByUser,
        Integer expectedDimensions
    ) throws IOException {
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

            PlainActionFuture<InferenceServiceResults> listener = createEmbeddingsFuture(
                sender,
                createWithEmptySettings(threadPool),
                user,
                dimensions,
                dimensionsSetByUser
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Llama text_embedding request from inference entity id [id]. Cause: Required [data]")
            );

            assertEmbeddingsRequest(user, expectedDimensions);
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForCompletionAction() throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForCompletionAction("overridden_user");
    }

    public void testExecute_ReturnsSuccessfulResponse_ForCompletionAction_WithoutUser() throws IOException {
        testExecute_ReturnsSuccessfulResponse_ForCompletionAction(null);
    }

    private void testExecute_ReturnsSuccessfulResponse_ForCompletionAction(String user) throws IOException {
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

            PlainActionFuture<InferenceServiceResults> listener = createCompletionFuture(sender, createWithEmptySettings(threadPool), user);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there, how may I assist you today?"))));

            assertCompletionRequest(user);
        }
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForCompletionAction() throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForCompletionAction("overridden_user");
    }

    public void testExecute_FailsFromInvalidResponseFormat_ForCompletionAction_WithoutUser() throws IOException {
        testExecute_FailsFromInvalidResponseFormat_ForCompletionAction(null);
    }

    private void testExecute_FailsFromInvalidResponseFormat_ForCompletionAction(String user) throws IOException {
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
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator()),
                user
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Llama completion request from inference entity id [id]. Cause: Required [choices]")
            );

            assertCompletionRequest(user);
        }
    }

    private PlainActionFuture<InferenceServiceResults> createEmbeddingsFuture(
        Sender sender,
        ServiceComponents threadPool,
        String user,
        Integer dimensions,
        boolean dimensionsSetByUser
    ) {
        var model = LlamaEmbeddingsModelTests.createEmbeddingsModel(
            "model",
            getUrl(webServer),
            "secret",
            user,
            dimensions,
            dimensionsSetByUser
        );
        var actionCreator = new LlamaActionCreator(sender, threadPool);
        var overriddenTaskSettings = createRequestTaskSettingsMap(user);
        var action = actionCreator.create(model, overriddenTaskSettings);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(
            new EmbeddingsInput(List.of("abc"), null, InputTypeTests.randomWithNull()),
            InferenceAction.Request.DEFAULT_TIMEOUT,
            listener
        );
        return listener;
    }

    private PlainActionFuture<InferenceServiceResults> createCompletionFuture(Sender sender, ServiceComponents threadPool, String user) {
        var model = LlamaChatCompletionModelTests.createCompletionModel("model", getUrl(webServer), "secret", user);
        var actionCreator = new LlamaActionCreator(sender, threadPool);
        var overriddenTaskSettings = createRequestTaskSettingsMap(user);
        var action = actionCreator.create(model, overriddenTaskSettings);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of("Hello"), false), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
        return listener;
    }

    private void assertCompletionRequest(String user) throws IOException {
        assertCommonRequestProperties();

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        if (user == null) {
            assertThat(requestMap.size(), is(4));
        } else {
            assertThat(requestMap.size(), is(5));
        }
        assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "Hello"))));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("n"), is(1));
        assertThat(requestMap.get("stream"), is(false));
        assertThat(requestMap.get("user"), is(user));
    }

    @SuppressWarnings("unchecked")
    private void assertEmbeddingsRequest(String user, Integer dimensions) throws IOException {
        assertCommonRequestProperties();

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        assertThat(requestMap.get("input"), instanceOf(List.class));
        var inputList = (List<String>) requestMap.get("input");
        assertThat(inputList, contains("abc"));
        assertThat(requestMap.get("user"), is(user));
        assertThat(requestMap.get("dimensions"), is(dimensions));
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
