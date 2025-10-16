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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
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
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.RankedDocsResultsTests.buildExpectationRerank;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OpenShiftAiActionCreatorTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
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

            var model = createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abc"), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testCreate_OpenShiftAiEmbeddingsModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
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
                  "object": "list",
                  "data_does_not_exist": [
                      {
                          "object": "embedding",
                          "index": 0,
                          "embedding": [
                              0.0123,
                              -0.0123
                          ]
                      }
                  ],
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abc"), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var failureCauseMessage = "Required [data]";
            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    format(
                        "Failed to send OpenShift AI text_embedding request from inference entity id [inferenceEntityId]. Cause: %s",
                        failureCauseMessage
                    )
                )
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
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

            var model = createCompletionModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("Hello there, how may I assist you today?"))));
            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);

            assertNull(request.getUri().getQuery());
            assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaTypeWithoutParameters()));
            assertThat(request.getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(4));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("n"), is(1));
            assertThat(requestMap.get("stream"), is(false));
        }
    }

    public void testCreate_OpenShiftAiChatCompletionModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
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

            var model = createCompletionModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var failureCauseMessage = "Required [choices]";
            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(
                    format(
                        "Failed to send OpenShift AI completion request from inference entity id [inferenceEntityId]. Cause: %s",
                        failureCauseMessage
                    )
                )
            );
            assertThat(thrownException.getCause().getMessage(), is(failureCauseMessage));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(4));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("n"), is(1));
            assertThat(requestMap.get("stream"), is(false));
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From413StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            var contentTooLargeErrorMessage =
                "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                    + "0 for the completion). Please reduce your prompt; or completion length.";

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

            var model = createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abcd"), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(2));
            {
                assertNull(webServer.requests().get(0).getUri().getQuery());
                assertThat(
                    webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                assertThat(requestMap.size(), is(2));
                assertThat(requestMap.get("input"), is(List.of("abcd")));
                assertThat(requestMap.get("model"), is("model"));
            }
            {
                assertNull(webServer.requests().get(1).getUri().getQuery());
                assertThat(
                    webServer.requests().get(1).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(1).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                assertThat(requestMap.size(), is(2));
                assertThat(requestMap.get("input"), is(List.of("ab")));
                assertThat(requestMap.get("model"), is("model"));
            }
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From400StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.startSynchronously();

            var contentTooLargeErrorMessage =
                "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                    + "0 for the completion). Please reduce your prompt; or completion length.";

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
            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("abcd"), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(2));
            {
                assertNull(webServer.requests().get(0).getUri().getQuery());
                assertThat(
                    webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                assertThat(requestMap.size(), is(2));
                assertThat(requestMap.get("input"), is(List.of("abcd")));
                assertThat(requestMap.get("model"), is("model"));
            }
            {
                assertNull(webServer.requests().get(1).getUri().getQuery());
                assertThat(
                    webServer.requests().get(1).getHeader(HttpHeaders.CONTENT_TYPE),
                    equalTo(XContentType.JSON.mediaTypeWithoutParameters())
                );
                assertThat(webServer.requests().get(1).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                assertThat(requestMap.size(), is(2));
                assertThat(requestMap.get("input"), is(List.of("ab")));
                assertThat(requestMap.get("model"), is("model"));
            }
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
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

            // truncated to 1 token = 3 characters
            var model = createModel(getUrl(webServer), "secret", "model", 1);
            var actionCreator = new OpenShiftAiActionCreator(sender, createWithEmptySettings(threadPool));
            var action = actionCreator.create(model);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new EmbeddingsInput(List.of("super long input"), InputTypeTests.randomWithNull()),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(
                webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
                equalTo(XContentType.JSON.mediaTypeWithoutParameters())
            );
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("sup")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testCreate_OpenShiftAiRerankModel() throws IOException {
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        List<String> documents = List.of("Luke");
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

            var model = OpenShiftAiRerankModelTests.createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs("popular name", documents, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            assertThat(
                result.asMap(),
                is(
                    buildExpectationRerank(
                        List.of(
                            new RankedDocsResultsTests.RerankExpectation(
                                Map.of("text", "awgawgawgawg", "index", 1, "relevance_score", 0.9921875f)
                            ),
                            new RankedDocsResultsTests.RerankExpectation(
                                Map.of("text", "awdawdawda", "index", 0, "relevance_score", 0.4921875f)
                            )
                        )
                    )
                )
            );
        }
        assertRerankActionCreator(documents);
    }

    public void testCreate_OpenShiftAiRerankModel_FailsFromInvalidResponseFormat() throws IOException {
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        List<String> documents = List.of("Luke");
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

            var model = OpenShiftAiRerankModelTests.createModel(getUrl(webServer), "secret", "model");
            var actionCreator = new OpenShiftAiActionCreator(
                sender,
                new ServiceComponents(threadPool, mockThrottlerManager(), settings, TruncatorTests.createTruncator())
            );
            var action = actionCreator.create(model, null);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs("popular name", documents, null, null, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is("""
                Failed to send OpenShift AI rerank request from inference entity id [inferenceEntityId]. Cause: Failed to find required\
                 field [results] in Cohere rerank response"""));
        }
        assertRerankActionCreator(documents);
    }

    private void assertRerankActionCreator(List<String> documents)
        throws IOException {
        assertThat(webServer.requests(), hasSize(1));
        assertNull(webServer.requests().get(0).getUri().getQuery());
        assertThat(
            webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE),
            equalTo(XContentType.JSON.mediaTypeWithoutParameters())
        );
        assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));

        var requestMap = entityAsMap(webServer.requests().get(0).getBody());
        assertThat(requestMap.size(), is(5));
        assertThat(requestMap.get("documents"), is(documents));
        assertThat(requestMap.get("model"), is("model"));
        assertThat(requestMap.get("query"), is("popular name"));
        assertThat(requestMap.get("top_n"), is(2));
        assertThat(requestMap.get("return_documents"), is(true));
    }
}
