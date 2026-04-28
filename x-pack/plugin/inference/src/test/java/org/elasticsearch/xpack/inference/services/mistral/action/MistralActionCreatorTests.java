/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.mistral.action;

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
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.mistral.MistralConstants;
import org.elasticsearch.xpack.inference.services.mistral.completion.MistralChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.mistral.embeddings.MistralEmbeddingModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGES_FIELD;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.external.unified.UnifiedChatCompletionRequestEntity.NUMBER_OF_RETURNED_CHOICES_FIELD;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class MistralActionCreatorTests extends ESTestCase {
    private static final String TEST_API_KEY = "secret";
    private static final String TEST_EMBEDDING_MODEL_ID = "mistral-embed";
    private static final String TEST_COMPLETION_MODEL_ID = "model";
    private static final String TEST_EMBEDDING_INPUT = "abc";
    private static final String TEST_CHAT_COMPLETION_INPUT = "Hello";

    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityExecutors());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testExecute_ReturnsSuccessfulResponse_ForEmbeddingAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                  "object": "list",
                  "data": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "mistral-embed",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = MistralEmbeddingModelTests.createModel(getUrl(webServer), "id", TEST_EMBEDDING_MODEL_ID, TEST_API_KEY);
            var actionCreator = new MistralActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(List.of(TEST_EMBEDDING_INPUT), InputTypeTests.randomWithNull()), null, listener);

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + TEST_API_KEY));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get(MistralConstants.INPUT_FIELD), is(List.of(TEST_EMBEDDING_INPUT)));
            assertThat(requestMap.get(MistralConstants.MODEL_FIELD), is(TEST_EMBEDDING_MODEL_ID));
            assertThat(requestMap.get(MistralConstants.ENCODING_FORMAT_FIELD), is("float"));
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_ForChatCompletionAction() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                     "object": "chat.completion",
                     "id": "",
                     "created": 1745855316,
                     "model": "/repository",
                     "system_fingerprint": "3.2.3-sha-a1f3ebe",
                     "choices": [
                         {
                             "index": 0,
                             "message": {
                                 "role": "assistant",
                                 "content": "Hello there, how may I assist you today?"
                             },
                             "logprobs": null,
                             "finish_reason": "stop"
                         }
                     ],
                     "usage": {
                         "prompt_tokens": 8,
                         "completion_tokens": 50,
                         "total_tokens": 58
                     }
                 }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createChatCompletionFuture(sender, createWithEmptySettings(threadPool));

            var result = listener.actionGet(TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there, how may I assist you today?"))));

            assertChatCompletionRequest();
        }
    }

    public void testSend_FailsFromInvalidResponseFormat_ForChatCompletionAction() throws IOException {
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "invalid_field": "unexpected"
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            PlainActionFuture<InferenceServiceResults> listener = createChatCompletionFuture(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Mistral completion request from inference entity id " + "[id]. Cause: Required [choices]")
            );

            assertChatCompletionRequest();
        }
    }

    private PlainActionFuture<InferenceServiceResults> createChatCompletionFuture(Sender sender, ServiceComponents serviceComponents) {
        var model = MistralChatCompletionModelTests.createCompletionModel(TEST_API_KEY, TEST_COMPLETION_MODEL_ID);
        model.setURI(getUrl(webServer));
        var actionCreator = new MistralActionCreator(sender, serviceComponents);
        var action = actionCreator.create(model);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        action.execute(new ChatCompletionInput(List.of(TEST_CHAT_COMPLETION_INPUT), false), null, listener);
        return listener;
    }

    private void assertChatCompletionRequest() throws IOException {
        assertThat(webServer.requests(), hasSize(1));
        assertNull(webServer.requests().get(0).getUri().getQuery());
        assertThat(
            webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
            equalTo(XContentType.JSON.mediaTypeWithoutParameters())
        );
        assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer " + TEST_API_KEY));

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        assertThat(requestMap.size(), is(4));
        assertThat(requestMap.get(MESSAGES_FIELD), is(List.of(Map.of("role", "user", "content", TEST_CHAT_COMPLETION_INPUT))));
        assertThat(requestMap.get(MistralConstants.MODEL_FIELD), is(TEST_COMPLETION_MODEL_ID));
        assertThat(requestMap.get(NUMBER_OF_RETURNED_CHOICES_FIELD), is(1));
        assertThat(requestMap.get(UnifiedChatCompletionRequestEntity.STREAM_FIELD), is(false));
    }
}
