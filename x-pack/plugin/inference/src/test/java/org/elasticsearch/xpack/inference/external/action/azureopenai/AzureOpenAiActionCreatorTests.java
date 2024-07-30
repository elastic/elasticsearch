/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.action.azureopenai;

import org.apache.http.HttpHeaders;
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
import org.elasticsearch.xpack.core.inference.results.ChatCompletionResults;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.DocumentsOnlyInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.request.azureopenai.AzureOpenAiUtils;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.azureopenai.completion.AzureOpenAiCompletionModelTests.createCompletionModel;
import static org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModelTests.createModel;
import static org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsRequestTaskSettingsTests.createRequestTaskSettingsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureOpenAiActionCreatorTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private static final Settings ZERO_TIMEOUT_SETTINGS = buildSettingsWithRetryFields(
        TimeValue.timeValueMillis(1),
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(0)
    );
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

    public void testCreate_AzureOpenAiEmbeddingsModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel("resource", "deployment", "apiversion", "orig_user", "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap("overridden_user");
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateEmbeddingsRequestMapWithUser(requestMap, List.of("abc"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testCreate_AzureOpenAiEmbeddingsModel_WithoutUser() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel("resource", "deployment", "apiversion", null, "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap(null);
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateEmbeddingsRequestMapWithUser(requestMap, List.of("abc"), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testCreate_AzureOpenAiEmbeddingsModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, ZERO_TIMEOUT_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.start();

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

            var model = createModel("resource", "deployment", "apiversion", null, "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap("overridden_user");
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(format("Failed to send Azure OpenAI embeddings request to [%s]", getUrl(webServer)))
            );
            assertThat(thrownException.getCause().getMessage(), is("Failed to find required field [data] in OpenAI embeddings response"));

            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateEmbeddingsRequestMapWithUser(requestMap, List.of("abc"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From413StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            // note - there is no complete documentation on Azure's error messages
            // but this error and response has been verified manually via CURL
            var contentTooLargeErrorMessage =
                "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                    + "0 for the completion). Please reduce your prompt; or completion length.";

            String responseJsonContentTooLarge = Strings.format("""
                    {
                        "error": {
                            "message": "%s",
                            "type": "invalid_request_error",
                            "param": null,
                            "code": null
                        }
                    }
                """, contentTooLargeErrorMessage);

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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(413).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel("resource", "deployment", "apiversion", null, "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap("overridden_user");
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abcd")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(2));
            {
                validateRequestWithApiKey(webServer.requests().get(0), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                validateEmbeddingsRequestMapWithUser(requestMap, List.of("abcd"), "overridden_user");
            }
            {
                validateRequestWithApiKey(webServer.requests().get(1), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                validateEmbeddingsRequestMapWithUser(requestMap, List.of("ab"), "overridden_user");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From400StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            // note - there is no complete documentation on Azure's error messages
            // but this error and response has been verified manually via CURL
            var contentTooLargeErrorMessage =
                "This model's maximum context length is 8192 tokens, however you requested 13531 tokens (13531 in your prompt;"
                    + "0 for the completion). Please reduce your prompt; or completion length.";

            String responseJsonContentTooLarge = Strings.format("""
                    {
                        "error": {
                            "message": "%s",
                            "type": "invalid_request_error",
                            "param": null,
                            "code": null
                        }
                    }
                """, contentTooLargeErrorMessage);

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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(400).setBody(responseJsonContentTooLarge));
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createModel("resource", "deployment", "apiversion", null, "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap("overridden_user");
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abcd")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(2));
            {
                validateRequestWithApiKey(webServer.requests().get(0), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                validateEmbeddingsRequestMapWithUser(requestMap, List.of("abcd"), "overridden_user");
            }
            {
                validateRequestWithApiKey(webServer.requests().get(1), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                validateEmbeddingsRequestMapWithUser(requestMap, List.of("ab"), "overridden_user");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

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
                  "model": "text-embedding-ada-002-v2",
                  "usage": {
                      "prompt_tokens": 8,
                      "total_tokens": 8
                  }
                }
                """;
            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            // truncated to 1 token = 3 characters
            var model = createModel("resource", "deployment", "apiversion", null, false, 1, null, null, "apikey", null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var overriddenTaskSettings = createRequestTaskSettingsMap("overridden_user");
            var action = actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("super long input")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateEmbeddingsRequestMapWithUser(requestMap, List.of("sup"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testInfer_AzureOpenAiCompletion_WithOverriddenUser() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "choices": [
                                {
                                    "finish_reason": "stop",
                                    "index": 0,
                                    "logprobs": null,
                                    "message": {
                                        "content": "response",
                                        "role": "assistant"
                                        }
                                    }
                                ],
                                "model": "gpt-4",
                                "object": "chat.completion"
                }""";

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var originalUser = "original_user";
            var overriddenUser = "overridden_user";
            var apiKey = "api_key";
            var completionInput = "some input";

            var model = createCompletionModel("resource", "deployment", "apiversion", originalUser, apiKey, null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var taskSettingsWithUserOverride = createRequestTaskSettingsMap(overriddenUser);
            var action = actionCreator.create(model, taskSettingsWithUserOverride);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of(completionInput)), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);
            var requestMap = entityAsMap(request.getBody());

            assertThat(
                result.asMap(),
                is(Map.of(ChatCompletionResults.COMPLETION, List.of(Map.of(ChatCompletionResults.Result.RESULT, "response"))))
            );
            validateRequestWithApiKey(request, apiKey);
            validateCompletionRequestMapWithUser(requestMap, List.of(completionInput), overriddenUser);

        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testInfer_AzureOpenAiCompletionModel_WithoutUser() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            String responseJson = """
                {
                    "choices": [
                                {
                                    "finish_reason": "stop",
                                    "index": 0,
                                    "logprobs": null,
                                    "message": {
                                        "content": "response",
                                        "role": "assistant"
                                        }
                                    }
                                ],
                                "model": "gpt-4",
                                "object": "chat.completion"
                }""";

            var completionInput = "some input";
            var apiKey = "api key";

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCompletionModel("resource", "deployment", "apiversion", null, apiKey, null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var requestTaskSettingsWithoutUser = createRequestTaskSettingsMap(null);
            var action = actionCreator.create(model, requestTaskSettingsWithoutUser);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of(completionInput)), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(webServer.requests(), hasSize(1));

            var request = webServer.requests().get(0);
            var requestMap = entityAsMap(request.getBody());

            assertThat(
                result.asMap(),
                is(Map.of(ChatCompletionResults.COMPLETION, List.of(Map.of(ChatCompletionResults.Result.RESULT, "response"))))
            );
            validateRequestWithApiKey(request, apiKey);
            validateCompletionRequestMapWithUser(requestMap, List.of(completionInput), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testInfer_AzureOpenAiCompletionModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, ZERO_TIMEOUT_SETTINGS);

        try (var sender = createSender(senderFactory)) {
            sender.start();

            // "choices" missing
            String responseJson = """
                {
                    "not_choices": [
                                   {
                                    "finish_reason": "stop",
                                    "index": 0,
                                    "logprobs": null,
                                    "message": {
                                        "content": "response",
                                        "role": "assistant"
                                        }
                                    }
                                ],
                                "model": "gpt-4",
                                "object": "chat.completion"
                }""";

            var completionInput = "some input";
            var apiKey = "api key";
            var userOverride = "overridden_user";

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(responseJson));

            var model = createCompletionModel("resource", "deployment", "apiversion", null, apiKey, null, "id");
            model.setUri(new URI(getUrl(webServer)));
            var actionCreator = new AzureOpenAiActionCreator(sender, createWithEmptySettings(threadPool));
            var requestTaskSettingsWithoutUser = createRequestTaskSettingsMap(userOverride);
            var action = actionCreator.create(model, requestTaskSettingsWithoutUser);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of(completionInput)), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var thrownException = expectThrows(ElasticsearchStatusException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(
                thrownException.getMessage(),
                is(format("Failed to send Azure OpenAI completion request to [%s]", getUrl(webServer)))
            );
            assertThat(
                thrownException.getCause().getMessage(),
                is("Failed to find required field [choices] in Azure OpenAI completions response")
            );

            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), apiKey);

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateCompletionRequestMapWithUser(requestMap, List.of(completionInput), userOverride);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateEmbeddingsRequestMapWithUser(Map<String, Object> requestMap, List<String> input, @Nullable String user) {
        var expectedSize = user == null ? 1 : 2;

        assertThat(requestMap.size(), is(expectedSize));
        assertThat(requestMap.get("input"), is(input));

        if (user != null) {
            assertThat(requestMap.get("user"), is(user));
        }
    }

    private void validateCompletionRequestMapWithUser(Map<String, Object> requestMap, List<String> input, @Nullable String user) {
        assertThat("input for completions can only be of size 1", input.size(), equalTo(1));

        var expectedSize = user == null ? 2 : 3;

        assertThat(requestMap.size(), is(expectedSize));
        assertThat(getContentOfMessageInRequestMap(requestMap), is(input.get(0)));

        if (user != null) {
            assertThat(requestMap.get("user"), is(user));
        }
    }

    @SuppressWarnings("unchecked")
    public static String getContentOfMessageInRequestMap(Map<String, Object> requestMap) {
        return ((Map<String, Object>) ((List<Object>) requestMap.get("messages")).get(0)).get("content").toString();
    }

    private void validateRequestWithApiKey(MockRequest request, String apiKey) {
        assertNull(request.getUri().getQuery());
        assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        assertThat(request.getHeader(AzureOpenAiUtils.API_KEY_HEADER), equalTo(apiKey));
    }
}
