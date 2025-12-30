/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.nvidia.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.common.model.Truncation;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.nvidia.completion.NvidiaChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.nvidia.rerank.NvidiaRerankModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
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
import static org.elasticsearch.xpack.inference.services.nvidia.embeddings.NvidiaEmbeddingsTaskSettingsTests.buildTaskSettingsMap;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.INPUT_TYPE_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.MODEL_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.PASSAGES_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.QUERY_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TEXT_FIELD_NAME;
import static org.elasticsearch.xpack.inference.services.nvidia.request.NvidiaRequestFields.TRUNCATE_FIELD_NAME;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class NvidiaActionCreatorTests extends ESTestCase {

    // Field names
    private static final String N_FIELD_NAME = "n";
    private static final String STREAM_FIELD_NAME = "stream";
    private static final String MESSAGES_FIELD_NAME = "messages";
    private static final String ROLE_FIELD_NAME = "role";
    private static final String CONTENT_FIELD_NAME = "content";

    // Test values
    private static final String API_KEY_VALUE = "test_api_key";
    private static final String MODEL_VALUE = "some_model";
    private static final String QUERY_VALUE = "popular name";
    private static final String ROLE_VALUE = "user";
    private static final String INPUT_ENTRY_VALUE = "abcd";
    private static final List<String> INPUT_VALUE = List.of(INPUT_ENTRY_VALUE);
    private static final String INPUT_TO_TRUNCATE = "super long input";
    private static final List<float[]> EMBEDDINGS_VALUE = List.of(new float[] { 0.0123F, -0.0123F }, new float[] { -0.0123F, 0.0123F });
    private static final String PASSAGE_VALUE = "Luke";
    private static final List<String> PASSAGES_VALUE = List.of(PASSAGE_VALUE);
    private static final int N_VALUE = 1;
    private static final List<RankedDocsResultsTests.RerankExpectation> RERANK_EXPECTATIONS_TWO_RESULTS = List.of(
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 1, "relevance_score", 0.9301758f)),
        new RankedDocsResultsTests.RerankExpectation(Map.of("index", 0, "relevance_score", -9.6953125f))
    );
    private static final String COMPLETION_RESULT_VALUE = "Hello there, how may I assist you today?";
    private static final String INPUT_TYPE_INITIAL_NVIDIA_VALUE = "passage";
    private static final String TRUNCATE_INITIAL_NVIDIA_VALUE = "start";
    private static final String INPUT_TYPE_OVERRIDDEN_NVIDIA_VALUE = "query";
    private static final String INPUT_TYPE_DEFAULT_NVIDIA_VALUE = "query";
    private static final String TRUNCATE_OVERRIDDEN_NVIDIA_VALUE = "end";
    private static final InputType INPUT_TYPE_INITIAL_ELASTIC_VALUE = InputType.INGEST;
    private static final Truncation TRUNCATE_INITIAL_ELASTIC_VALUE = Truncation.START;
    private static final InputType INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE = InputType.SEARCH;
    private static final Truncation TRUNCATE_OVERRIDDEN_ELASTIC_VALUE = Truncation.END;

    // Settings with no retries
    private static final Settings NO_RETRY_SETTINGS = buildSettingsWithRetryFields(
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(0)
    );
    private static final String EMBEDDING_RESPONSE_JSON = """
        {
            "object": "list",
            "data": [{
                    "index": 0,
                    "embedding": [
                        0.0123,
                        -0.0123
                    ],
                    "object": "embedding"
                }, {
                    "index": 1,
                    "embedding": [
                        -0.0123,
                        0.0123
                    ],
                    "object": "embedding"
                }
            ],
            "model": "some_model",
            "usage": {
                "prompt_tokens": 6,
                "total_tokens": 6
            }
        }
        """;
    private static final String CHAT_COMPLETION_RESPONSE_JSON = """
        {
            "id": "cmpl-ef780b96e82d46ceb1e45ee4188913bc",
            "object": "chat.completion",
            "created": 1752751862,
            "model": "%s",
            "choices": [
                {
                    "index": 0,
                    "message": {
                        "role": "assistant",
                        "content": "%s"
                    },
                    "logprobs": null,
                    "finish_reason": "stop",
                    "stop_reason": null
                }
            ],
            "usage": {
                "prompt_tokens": 9,
                "total_tokens": 31,
                "completion_tokens": 22
            }
        }
        """;
    private static final String INPUT_IS_TOO_LONG_ERROR_MESSAGE = """
            {
                "error": "Input length is too long"
            }
        """;

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

    public void testCreate_NvidiaEmbeddingsModel_NoTaskSettings_NoRequestInputType() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_DEFAULT_NVIDIA_VALUE, null);
        }
    }

    public void testCreate_NvidiaEmbeddingsModel_WithTaskSettings() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                INPUT_TYPE_INITIAL_ELASTIC_VALUE,
                TRUNCATE_INITIAL_ELASTIC_VALUE,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_INITIAL_NVIDIA_VALUE, TRUNCATE_INITIAL_NVIDIA_VALUE);
        }
    }

    public void testCreate_NvidiaEmbeddingsModel_WithOverriddenTaskSettings() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                INPUT_TYPE_INITIAL_ELASTIC_VALUE,
                TRUNCATE_INITIAL_ELASTIC_VALUE,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(
                model,
                buildTaskSettingsMap(INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE, TRUNCATE_OVERRIDDEN_ELASTIC_VALUE)
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_OVERRIDDEN_NVIDIA_VALUE, TRUNCATE_OVERRIDDEN_NVIDIA_VALUE);
        }
    }

    public void testCreate_NvidiaEmbeddingsModel_NoTaskSettings_WithRequestInputTypeParam() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_OVERRIDDEN_NVIDIA_VALUE, null);
        }
    }

    public void testCreate_NvidiaEmbeddingsModel_WithTaskSettings_WithRequestInputTypeParamPrioritized() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                INPUT_TYPE_INITIAL_ELASTIC_VALUE,
                TRUNCATE_INITIAL_ELASTIC_VALUE,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_OVERRIDDEN_NVIDIA_VALUE, TRUNCATE_INITIAL_NVIDIA_VALUE);
        }
    }

    public void testCreate_NvidiaEmbeddingsModel_WithRequestTaskSettings_WithRequestInputTypeParamPrioritized() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(
                model,
                buildTaskSettingsMap(INPUT_TYPE_INITIAL_ELASTIC_VALUE, TRUNCATE_INITIAL_ELASTIC_VALUE)
            );

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(INPUT_VALUE, INPUT_TYPE_OVERRIDDEN_ELASTIC_VALUE),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertEmbeddingsRequests(INPUT_TYPE_OVERRIDDEN_NVIDIA_VALUE, TRUNCATE_INITIAL_NVIDIA_VALUE);
        }
    }

    private void assertEmbeddingsRequests(String expectedInputType, String expectedTruncation) throws IOException {
        assertThat(webServer.requests(), hasSize(1));

        var request = webServer.requests().getFirst();
        assertThat(request.getUri().getQuery(), is(nullValue()));
        assertContentTypeAndAuthorization(request);

        var requestMap = entityAsMap(request.getBody());
        int size = 3;
        assertThat(requestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
        assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(expectedInputType));
        if (expectedTruncation != null) {
            size++;
            assertThat(requestMap.get(TRUNCATE_FIELD_NAME), is(expectedTruncation));
        }
        assertThat(requestMap, aMapWithSize(size));
    }

    public void testCreate_NvidiaEmbeddingsModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "object": "list",
                    "not_data": [{
                            "index": 0,
                            "embedding": [
                                0.0123,
                                -0.0123
                            ],
                            "object": "embedding"
                        }, {
                            "index": 1,
                            "embedding": [
                                -0.0123,
                                0.0123
                            ],
                            "object": "embedding"
                        }
                    ],
                    "model": "some_model",
                    "usage": {
                        "prompt_tokens": 6,
                        "total_tokens": 6
                    }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)
            );

            var failureCauseMessage = "Required [data]";
            assertThat(
                thrownException.getMessage(),
                is(
                    Strings.format(
                        "Failed to send Nvidia text_embedding request from inference entity id [inferenceEntityId]. Cause: %s",
                        failureCauseMessage
                    )
                )
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));
        }
    }

    public void testCreate_NvidiaChatCompletionModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = Strings.format(CHAT_COMPLETION_RESPONSE_JSON, MODEL_VALUE, COMPLETION_RESULT_VALUE);

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(INPUT_VALUE), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of(COMPLETION_RESULT_VALUE))));
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

    public void testCreate_NvidiaChatCompletionModel_413StatusCode_ThrowsException() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(INPUT_IS_TOO_LONG_ERROR_MESSAGE));

            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(INPUT_VALUE), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)
            );
            assertThat(thrownException.getMessage(), is(Strings.format("""
                Received a content too large status code for request from inference entity id [inferenceEntityId] status [413]. \
                Error message: [%s]""", INPUT_IS_TOO_LONG_ERROR_MESSAGE)));
        }
    }

    public void testCreate_NvidiaChatCompletionModel_400StatusCodeWithContentTooLargeMessage_ThrowsException() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJsonContentTooLarge = """
                    {
                        "error": "Please reduce your prompt; or completion length."
                    }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJsonContentTooLarge));

            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(INPUT_VALUE), InferenceAction.Request.DEFAULT_TIMEOUT, listener);
            var thrownException = expectThrows(
                ElasticsearchStatusException.class,
                () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT)
            );
            assertThat(thrownException.getMessage(), is(Strings.format("""
                Received a content too large status code for request from inference entity id [inferenceEntityId] status [400]. \
                Error message: [%s]""", responseJsonContentTooLarge)));
        }
    }

    public void testCreate_NvidiaChatCompletionModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = Strings.format("""
                {
                    "id": "cmpl-ef780b96e82d46ceb1e45ee4188913bc",
                    "object": "chat.completion",
                    "created": 1752751862,
                    "model": "%s",
                    "not_choices": [
                        {
                            "index": 0,
                            "message": {
                                "role": "assistant",
                                "content": "%s"
                            },
                            "logprobs": null,
                            "finish_reason": "stop",
                            "stop_reason": null
                        }
                    ],
                    "usage": {
                        "prompt_tokens": 9,
                        "total_tokens": 31,
                        "completion_tokens": 22
                    }
                }
                """, MODEL_VALUE, COMPLETION_RESULT_VALUE);
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = NvidiaChatCompletionModelTests.createCompletionModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model);

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
                        "Failed to send Nvidia completion request from inference entity id [inferenceEntityId]. Cause: %s",
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

            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(INPUT_IS_TOO_LONG_ERROR_MESSAGE));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var actionCreator = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(2));
            {
                assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
                assertContentTypeAndAuthorization(webServer.requests().getFirst());

                var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
                assertThat(requestMap, aMapWithSize(3));
                assertThat(requestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
                assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
                assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_DEFAULT_NVIDIA_VALUE));
            }
            {
                assertThat(webServer.requests().get(1).getUri().getQuery(), is(nullValue()));
                assertContentTypeAndAuthorization(webServer.requests().get(1));

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                assertThat(requestMap, aMapWithSize(3));
                assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_ENTRY_VALUE.substring(0, 2))));
                assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
                assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_DEFAULT_NVIDIA_VALUE));
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From400StatusCodeWithContentTooLargeMessage() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJsonContentTooLarge = """
                    {
                        "error": "Input length 18432 exceeds maximum allowed token size 8192"
                    }
                """;

            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                null,
                null,
                null,
                null,
                null
            );
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new EmbeddingsInput(INPUT_VALUE, null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(2));

            {
                var firstRequest = webServer.requests().getFirst();
                assertContentTypeAndAuthorization(firstRequest);
                var firstRequestMap = entityAsMap(firstRequest.getBody());
                assertThat(firstRequestMap, aMapWithSize(3));
                assertThat(firstRequestMap.get(INPUT_FIELD_NAME), is(INPUT_VALUE));
                assertThat(firstRequestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
                assertThat(firstRequestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_DEFAULT_NVIDIA_VALUE));
            }
            {
                var secondRequest = webServer.requests().get(1);
                assertContentTypeAndAuthorization(secondRequest);
                var secondRequestMap = entityAsMap(secondRequest.getBody());
                assertThat(secondRequestMap, aMapWithSize(3));
                assertThat(secondRequestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_ENTRY_VALUE.substring(0, 2))));
                assertThat(secondRequestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
                assertThat(secondRequestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_DEFAULT_NVIDIA_VALUE));
            }
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(EMBEDDING_RESPONSE_JSON));

            // truncated to 1 token = 3 characters
            var model = NvidiaEmbeddingsModelTests.createEmbeddingsModel(
                getUrl(webServer),
                API_KEY_VALUE,
                MODEL_VALUE,
                1,
                null,
                null,
                null,
                null
            );
            var action = new NvidiaActionCreator(sender, createWithEmptySettings(threadPool)).create(model, null);

            var listener = new PlainActionFuture<InferenceServiceResults>();
            action.execute(new EmbeddingsInput(List.of(INPUT_TO_TRUNCATE), null), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(EMBEDDINGS_VALUE)));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().getFirst();
            assertThat(request.getUri().getQuery(), is(nullValue()));
            assertContentTypeAndAuthorization(request);

            var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap, aMapWithSize(3));
            assertThat(requestMap.get(INPUT_FIELD_NAME), is(List.of(INPUT_TO_TRUNCATE.substring(0, 3))));
            assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
            assertThat(requestMap.get(INPUT_TYPE_FIELD_NAME), is(INPUT_TYPE_DEFAULT_NVIDIA_VALUE));
        }
    }

    public void testCreate_NvidiaRerankModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "rankings": [
                        {
                            "index": 1,
                            "logit": 0.93017578125
                        },
                        {
                            "index": 0,
                            "logit": -9.6953125
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = NvidiaRerankModelTests.createRerankModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var actionCreator = new NvidiaActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, PASSAGES_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT);
            assertThat(result.asMap(), is(buildExpectationRerank(RERANK_EXPECTATIONS_TWO_RESULTS)));
        }
        assertThat(webServer.requests(), hasSize(1));
        assertThat(webServer.requests().getFirst().getUri().getQuery(), is(nullValue()));
        assertContentTypeAndAuthorization(webServer.requests().getFirst());

        var requestMap = entityAsMap(webServer.requests().getFirst().getBody());
        assertThat(requestMap, aMapWithSize(3));
        assertThat(requestMap.get(PASSAGES_FIELD_NAME), is(List.of(Map.of(TEXT_FIELD_NAME, PASSAGE_VALUE))));
        assertThat(requestMap.get(QUERY_FIELD_NAME), is(Map.of(TEXT_FIELD_NAME, QUERY_VALUE)));
        assertThat(requestMap.get(MODEL_FIELD_NAME), is(MODEL_VALUE));
    }

    public void testCreate_NvidiaRerankModel_FailsFromInvalidResponseFormat() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, NO_RETRY_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            String responseJson = """
                {
                    "not_rankings": [
                        {
                            "index": 1,
                            "logit": 0.93017578125
                        },
                        {
                            "index": 0,
                            "logit": -9.6953125
                        }
                    ]
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = NvidiaRerankModelTests.createRerankModel(getUrl(webServer), API_KEY_VALUE, MODEL_VALUE);
            var actionCreator = new NvidiaActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), NO_RETRY_SETTINGS, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(QUERY_VALUE, PASSAGES_VALUE, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(ESTestCase.TEST_REQUEST_TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is("Failed to send Nvidia rerank request from inference entity id [inferenceEntityId]. Cause: Required [rankings]")
            );
        }
    }

    private static void assertContentTypeAndAuthorization(MockRequest request) {
        assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), is(XContentType.JSON.mediaTypeWithoutParameters()));
        assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), is(Strings.format("Bearer %s", API_KEY_VALUE)));
    }

}
