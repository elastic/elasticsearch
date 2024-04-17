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
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSenderWithSingleRequestManager;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectation;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsModelTests.createModel;
import static org.elasticsearch.xpack.inference.services.azureopenai.embeddings.AzureOpenAiEmbeddingsRequestTaskSettingsTests.getRequestTaskSettingsMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureOpenAiActionCreatorTests extends ESTestCase {
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

    public void testCreate_AzureOpenAiEmbeddingsModel() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap("overridden_user");
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateRequestMapWithUser(requestMap, List.of("abc"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testCreate_AzureOpenAiEmbeddingsModel_WithoutUser() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap(null);
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateRequestMapWithUser(requestMap, List.of("abc"), null);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testCreate_AzureOpenAiEmbeddingsModel_FailsFromInvalidResponseFormat() throws IOException {
        // timeout as zero for no retries
        var settings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager, settings);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap("overridden_user");
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

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
            validateRequestMapWithUser(requestMap, List.of("abc"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From413StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap("overridden_user");
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abcd")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));
            assertThat(webServer.requests(), hasSize(2));
            {
                validateRequestWithApiKey(webServer.requests().get(0), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                validateRequestMapWithUser(requestMap, List.of("abcd"), "overridden_user");
            }
            {
                validateRequestWithApiKey(webServer.requests().get(1), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                validateRequestMapWithUser(requestMap, List.of("ab"), "overridden_user");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_ReturnsSuccessfulResponse_AfterTruncating_From400StatusCode() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap("overridden_user");
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("abcd")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));
            assertThat(webServer.requests(), hasSize(2));
            {
                validateRequestWithApiKey(webServer.requests().get(0), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(0).getBody());
                validateRequestMapWithUser(requestMap, List.of("abcd"), "overridden_user");
            }
            {
                validateRequestWithApiKey(webServer.requests().get(1), "apikey");

                var requestMap = entityAsMap(webServer.requests().get(1).getBody());
                validateRequestMapWithUser(requestMap, List.of("ab"), "overridden_user");
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public void testExecute_TruncatesInputBeforeSending() throws IOException {
        var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);

        try (var sender = createSenderWithSingleRequestManager(senderFactory)) {
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
            var overriddenTaskSettings = getRequestTaskSettingsMap("overridden_user");
            var action = (AzureOpenAiEmbeddingsAction) actionCreator.create(model, overriddenTaskSettings);

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new DocumentsOnlyInput(List.of("super long input")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));
            assertThat(webServer.requests(), hasSize(1));
            validateRequestWithApiKey(webServer.requests().get(0), "apikey");

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            validateRequestMapWithUser(requestMap, List.of("sup"), "overridden_user");
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private void validateRequestMapWithUser(Map<String, Object> requestMap, List<String> input, @Nullable String user) {
        var expectedSize = user == null ? 1 : 2;

        assertThat(requestMap.size(), is(expectedSize));
        assertThat(requestMap.get("input"), is(input));

        if (user != null) {
            assertThat(requestMap.get("user"), is(user));
        }
    }

    private void validateRequestWithApiKey(MockRequest request, String apiKey) {
        assertNull(request.getUri().getQuery());
        assertThat(request.getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        assertThat(request.getHeader(AzureOpenAiUtils.API_KEY_HEADER), equalTo(apiKey));
    }
}
