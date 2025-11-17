/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.openshiftai.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankModelTests;
import org.elasticsearch.xpack.inference.services.openshiftai.rerank.OpenShiftAiRerankTaskSettings;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.DenseEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityExecutors;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.openshiftai.completion.OpenShiftAiChatCompletionModelTests.createCompletionModel;
import static org.elasticsearch.xpack.inference.services.openshiftai.embeddings.OpenShiftAiEmbeddingsModelTests.createModel;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class OpenShiftAiActionCreatorTests extends ESTestCase {

    // Field names
    private static final String N_FIELD_NAME = "n";
    private static final String STREAM_FIELD_NAME = "stream";
    private static final String MESSAGES_FIELD_NAME = "messages";
    private static final String ROLE_FIELD_NAME = "role";
    private static final String CONTENT_FIELD_NAME = "content";

    private static final String DOCUMENTS_FIELD_NAME = "documents";
    private static final String MODEL_FIELD_NAME = "model";
    private static final String QUERY_FIELD_NAME = "query";
    private static final String TOP_N_FIELD_NAME = "top_n";
    private static final String RETURN_DOCUMENTS_FIELD_NAME = "return_documents";

    private static final String INPUT_FIELD_NAME = "input";

    // Test values
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String MODEL_VALUE = "some_model";
    private static final String QUERY_VALUE = "popular name";
    private static final String ROLE_VALUE = "user";
    private static final String INPUT_ENTRY_VALUE = "abcd";
    private static final List<String> INPUT_VALUE = List.of(INPUT_ENTRY_VALUE);
    private static final String INPUT_TO_TRUNCATE = "super long input";
    private static final List<float[]> EMBEDDINGS_VALUE = List.of(new float[] { 0.0123F, -0.0123F });
    private static final List<String> DOCUMENTS_VALUE = List.of("Luke");
    private static final int N_VALUE = 1;
    private static final boolean RETURN_DOCUMENTS_DEFAULT_VALUE = true;
    private static final boolean RETURN_DOCUMENTS_OVERRIDDEN_VALUE = false;
    private static final int TOP_N_DEFAULT_VALUE = 2;
    private static final int TOP_N_OVERRIDDEN_VALUE = 1;
    private static final List<RankedDocsResultsTests.RerankExpectation> RERANK_EXPECTATIONS_WITH_TEXT_TWO_RESULTS = List.of(
        new RankedDocsResultsTests.RerankExpectation(Map.of("text", "awgawgawgawg", "index", 1, "relevance_score", 0.9921875f)),
        new RankedDocsResultsTests.RerankExpectation(Map.of("text", "awdawdawda", "index", 0, "relevance_score", 0.4921875f))
    );
    private static final List<RankedDocsResultsTests.RerankExpectation> RERANK_EXPECTATIONS_NO_TEXT_SINGLE_RESULT = List.of(
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 0, "relevance_score", 0.4921875f))
    );

    // Settings with no retries
    private static final Settings NO_RETRY_SETTINGS = buildSettingsWithRetryFields(
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(0)
    );

    // Mock server and client manager
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

    public void testCreate_OpenShiftAiEmbeddingsModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
                    "data": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0123,
                                -0.0123
                            ]
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().getFirst();
            assertThat(request.getUri().getQuery(), is(nullValue()));
            assertContentTypeAndAuthorization(request);

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, aMapWithSize(2));
            assertThat(requestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        }
    }

    private static void assertContentTypeAndAuthorization(MockRequest request) {
        assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), equalTo(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

    public void testCreate_OpenShiftAiEmbeddingsModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
                    "data_does_not_exist": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0123,
                                -0.0123
                            ]
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)
            );

            var failureCauseMessage = "Required [data]";
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        "Failed to send OpenShift AI text_embedding request from inference entity id [inferenceEntityId]. Cause: %s",
                        failureCauseMessage
                    )
                )
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));
        }
    }

    public void testCreate_OpenShiftAiChatCompletionModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "chatcmpl-921d2eb8f3bc46dd8f4cb0502a4608a7",
                    "object": "chat.completion",
                    "created": 1760082857,
                    "model": "llama-31-8b-instruct",
                    "choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "reasoning_content": null,
                                "content": "Hello there, how may I assist you today?",
                                "tool_calls": []
                            },
                            "logprobs": null,
                            "finish_reason": "length",
                            "stop_reason": null
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 40,
                        "total_tokens": 140,
                        "completion_tokens": 100,
                        "prompt_tokens_details": null
                    },
                    "prompt_logprobs": null,
                    "kv_transfer_params": null
                }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(INPUT_VALUE), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there, how may I assist you today?"))));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().getFirst();
            assertContentTypeAndAuthorization(request);

            var requestMap = entityAsMap(request.getBody());
            assertThat(
                requestMap.get(MESSAGES_FIELD_NAME),
                is(List.of(Map.of(ROLE_FIELD_NAME, ROLE_VALUE, CONTENT_FIELD_NAME, INPUT_ENTRY_VALUE)))
            );
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            assertThat(requestMap.get(N_FIELD_NAME), is(N_VALUE));
            assertThat(requestMap.get(STREAM_FIELD_NAME), is(false));
        }
    }

    public void testCreate_OpenShiftAiChatCompletionModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "chatcmpl-921d2eb8f3bc46dd8f4cb0502a4608a7",
                    "object": "chat.completion",
                    "created": 1760082857,
                    "model": "llama-31-8b-instruct",
                    "not_choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "reasoning_content": null,
                                "content": "Hello there, how may I assist you today?",
                                "tool_calls": []
                            },
                            "logprobs": null,
                            "finish_reason": "length",
                            "stop_reason": null
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 40,
                        "total_tokens": 140,
                        "completion_tokens": 100,
                        "prompt_tokens_details": null
                    },
                    "prompt_logprobs": null,
                    "kv_transfer_params": null
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(INPUT_VALUE), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)
            );
            var failureCauseMessage = "Required [choices]";
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        "Failed to send OpenShift AI completion request from inference entity id [inferenceEntityId]. Cause: %s",
                        failureCauseMessage
                    )
                )
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From413StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            var contentTooLargeErrorMessage = """
                This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;\
                0 for the completion). Please reduce your prompt; or completion length.""";

            String responseJsonContentTooLarge = Strings.format("""
                    {
                        "error": {
                            "message": "%s",
                            "type": "content_too_large",
                            "param": null,
                            "code": null
                        }
                    }
                """, contentTooLargeErrorMessage);

            String responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
                    "data": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0123,
                                -0.0123
                            ]
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(2));
            {
                assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
                assertContentTypeAndAuthorization(webServer.requests().getFirst());

                var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
                assertThat(requestMap, aMapWithSize(2));
                assertThat(requestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
                assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            }
            {
                assertThat(webServer.requests().get(1).getUri().getQuery(), is(nullValue()));
                assertContentTypeAndAuthorization(webServer.requests().get(1));

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                assertThat(requestMap, aMapWithSize(2));
                assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_ENTRY_VALUE.substring(0, 2))));
                assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From400StatusCodeWithContentTooLargeMessage() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            var contentTooLargeErrorMessage = """
                This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;\
                0 for the completion). Please reduce your prompt; or completion length.""";

            String responseJsonContentTooLarge = Strings.format("""
                    {
                        "error": {
                            "message": "%s",
                            "type": "content_too_large",
                            "param": null,
                            "code": null
                        }
                    }
                """, contentTooLargeErrorMessage);

            var responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
                    "data": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0123,
                                -0.0123
                            ]
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(2));

            {
                var firstRequest = webServer.requests().getFirst();
                assertContentTypeAndAuthorization(firstRequest);
                var firstRequestMap = entityAsMap(firstRequest.getBody());
                assertThat(firstRequestMap, aMapWithSize(2));
                assertThat(firstRequestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
                assertThat(firstRequestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            }
            {
                var secondRequest = webServer.requests().get(1);
                assertContentTypeAndAuthorization(secondRequest);
                var secondRequestMap = entityAsMap(secondRequest.getBody());
                assertThat(secondRequestMap, aMapWithSize(2));
                assertThat(secondRequestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_ENTRY_VALUE.substring(0, 2))));
                assertThat(secondRequestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            }
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            var responseJson = """
                {
                    "id": "embd-45e6d99b97a645c0af96653598069cd9",
                    "object": "list",
                    "created": 1760085467,
                    "model": "gritlm-7b",
                    "data": [
                        {
                            "index": 0,
                            "object": "embedding",
                            "embedding": [
                                0.0123,
                                -0.0123
                            ]
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 7,
                        "total_tokens": 7,
                        "completion_tokens": 0,
                        "prompt_tokens_details": null
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // truncated to 1 token = 3 characters
            var model = createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE, 1);
            var action = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            var listener = new PlainActionFuture<InferenceServiceResults>();
            action.execute(
                new EmbeddingsInput(List.of(INPUT_TO_TRUNCATE), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().getFirst();
            assertThat(request.getUri().getQuery(), is(nullValue()));
            assertContentTypeAndAuthorization(request);

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, aMapWithSize(2));
            assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_TO_TRUNCATE.substring(0, 3))));
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        }
    }

    public void testCreate_OpenShiftAiRerankModel_WithTaskSettings() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 30
                    },
                    "results": [
                        {
                            "index": 1,
                            "document": {
                                "text": "awgawgawgawg"
                            },
                            "relevance_score": 0.9921875
                        },
                        {
                            "index": 0,
                            "document": {
                                "text": "awdawdawda"
                            },
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                TOP_N_DEFAULT_VALUE,
                RETURN_DOCUMENTS_DEFAULT_VALUE
            );
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_WITH_TEXT_TWO_RESULTS)));
        }
        assertRerankActionCreator(TOP_N_DEFAULT_VALUE, RETURN_DOCUMENTS_DEFAULT_VALUE, MODEL_VALUE);
    }

    public void testCreate_OpenShiftAiRerankModel_WithOverriddenTaskSettings() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 10
                    },
                    "results": [
                        {
                            "index": 0,
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                TOP_N_DEFAULT_VALUE,
                RETURN_DOCUMENTS_DEFAULT_VALUE
            );
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(
                model,
                new HashMap<>(
                    Map.of(
                        OpenShiftAiRerankTaskSettings.RETURN_DOCUMENTS,
                        RETURN_DOCUMENTS_OVERRIDDEN_VALUE,
                        OpenShiftAiRerankTaskSettings.TOP_N,
                        TOP_N_OVERRIDDEN_VALUE
                    )
                )
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_NO_TEXT_SINGLE_RESULT)));
        }
        assertRerankActionCreator(TOP_N_OVERRIDDEN_VALUE, RETURN_DOCUMENTS_OVERRIDDEN_VALUE, MODEL_VALUE);
    }

    public void testCreate_OpenShiftAiRerankModel_NoTaskSettingsWithModelId() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 30
                    },
                    "results": [
                        {
                            "index": 1,
                            "document": {
                                "text": "awgawgawgawg"
                            },
                            "relevance_score": 0.9921875
                        },
                        {
                            "index": 0,
                            "document": {
                                "text": "awdawdawda"
                            },
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE, null, null);
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_WITH_TEXT_TWO_RESULTS)));
        }
        assertRerankActionCreator(null, null, MODEL_VALUE);
    }

    public void testCreate_OpenShiftAiRerankModel_NoTaskSettings_NoModelId() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 30
                    },
                    "results": [
                        {
                            "index": 1,
                            "document": {
                                "text": "awgawgawgawg"
                            },
                            "relevance_score": 0.9921875
                        },
                        {
                            "index": 0,
                            "document": {
                                "text": "awdawdawda"
                            },
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, null, null, null);
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_WITH_TEXT_TWO_RESULTS)));
        }
        assertRerankActionCreator(null, null, null);
    }

    public void testCreate_OpenShiftAiRerankModel_NoTaskSettings_WithRequestParameters() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 30
                    },
                    "results": [
                        {
                            "index": 1,
                            "document": {
                                "text": "awgawgawgawg"
                            },
                            "relevance_score": 0.9921875
                        },
                        {
                            "index": 0,
                            "document": {
                                "text": "awdawdawda"
                            },
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE, null, null);
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, RETURN_DOCUMENTS_DEFAULT_VALUE, TOP_N_DEFAULT_VALUE, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_WITH_TEXT_TWO_RESULTS)));
        }
        assertRerankActionCreator(TOP_N_DEFAULT_VALUE, RETURN_DOCUMENTS_DEFAULT_VALUE, MODEL_VALUE);
    }

    public void testCreate_OpenShiftAiRerankModel_WithTaskSettings_WithRequestParametersPrioritized() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 10
                    },
                    "results": [
                        {
                            "index": 0,
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                TOP_N_DEFAULT_VALUE,
                RETURN_DOCUMENTS_DEFAULT_VALUE
            );
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, RETURN_DOCUMENTS_OVERRIDDEN_VALUE, TOP_N_OVERRIDDEN_VALUE, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_NO_TEXT_SINGLE_RESULT)));
        }
        assertRerankActionCreator(TOP_N_OVERRIDDEN_VALUE, RETURN_DOCUMENTS_OVERRIDDEN_VALUE, MODEL_VALUE);
    }

    public void testCreate_OpenShiftAiRerankModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "id": "rerank-d300256dd02b4c63b8a2bc34dcdad845",
                    "model": "bge-reranker-v2-m3",
                    "usage": {
                        "total_tokens": 30
                    },
                    "not_results": [
                        {
                            "index": 1,
                            "document": {
                                "text": "awgawgawgawg"
                            },
                            "relevance_score": 0.9921875
                        },
                        {
                            "index": 0,
                            "document": {
                                "text": "awdawdawda"
                            },
                            "relevance_score": 0.4921875
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = OpenShiftAiRerankModelTests.createModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                TOP_N_DEFAULT_VALUE,
                RETURN_DOCUMENTS_DEFAULT_VALUE
            );
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, DOCUMENTS_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send OpenShift AI rerank request from inference entity id [inferenceEntityId]. Cause: Required [results]")
            );
        }
    }

    private void assertRerankActionCreator(
        @Nullable Integer expectedTopN,
        @Nullable Boolean expectedReturnDocuments,
        @Nullable String expectedModel
    ) throws IOException {
        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
        assertContentTypeAndAuthorization(webServer.requests().getFirst());

        var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
        int fieldCount = 2;
        assertThat(requestMap.get(DOCUMENTS_FIELD_NAME), is(DOCUMENTS_VALUE));
        assertThat(requestMap.get(QUERY_FIELD_NAME), is(QUERY_VALUE));
        if (expectedTopN != null) {
            assertThat(requestMap.get(TOP_N_FIELD_NAME), is(expectedTopN));
            fieldCount++;
        }
        if (expectedReturnDocuments != null) {
            assertThat(requestMap.get(RETURN_DOCUMENTS_FIELD_NAME), is(expectedReturnDocuments));
            fieldCount++;
        }
        if (expectedModel != null) {
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(expectedModel));
            fieldCount++;
        }
        assertThat(requestMap, aMapWithSize(fieldCount));
    }
}
