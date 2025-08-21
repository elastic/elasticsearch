/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.azureaistudio.action;

import org.apache.http.HttpHeaders;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.xpack.inference.InputTypeTests;
import org.elasticsearch.xpack.inference.common.TruncatorTests;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.ChatCompletionInput;
import org.elasticsearch.xpack.inference.external.http.sender.EmbeddingsInput;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSender;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests;
import org.elasticsearch.xpack.inference.external.http.sender.QueryAndDocsInputs;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioEndpointType;
import org.elasticsearch.xpack.inference.services.azureaistudio.AzureAiStudioProvider;
import org.elasticsearch.xpack.inference.services.azureaistudio.completion.AzureAiStudioChatCompletionModelTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.embeddings.AzureAiStudioEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.azureaistudio.rerank.AzureAiStudioRerankModelTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResultsTests.buildExpectationCompletion;
import static org.elasticsearch.xpack.core.inference.results.TextEmbeddingFloatResultsTests.buildExpectationFloat;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderTests.createSender;
import static org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioEmbeddingsRequestEntity.convertToString;
import static org.elasticsearch.xpack.inference.services.azureaistudio.request.AzureAiStudioRequestFields.API_KEY_HEADER;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class AzureAiStudioActionAndCreatorTests extends ESTestCase {
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

    public void testEmbeddingsRequestAction() throws IOException {
        final var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            clientManager,
            mockClusterServiceEmpty()
        );

        final var serviceComponents = getServiceComponents();

        try (var sender = createSender(senderFactory)) {
            sender.start();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testEmbeddingsTokenResponseJson));

            final var model = AzureAiStudioEmbeddingsModelTests.createModel(
                "id",
                "http://will-be-replaced.local",
                AzureAiStudioProvider.OPENAI,
                AzureAiStudioEndpointType.TOKEN,
                "apikey"
            );
            model.setURI(getUrl(webServer));

            final var creator = new AzureAiStudioActionCreator(sender, serviceComponents);
            final var action = creator.create(model, Map.of());
            final PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            final var inputType = InputTypeTests.randomSearchAndIngestWithNull();
            action.execute(new EmbeddingsInput(List.of("abc"), null, inputType), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            final var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationFloat(List.of(new float[] { 0.0123F, -0.0123F }))));
            assertWebServerRequest(API_KEY_HEADER, "apikey");

            final var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(InputType.isSpecified(inputType) ? 2 : 1));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            if (InputType.isSpecified(inputType)) {
                var convertedInputType = convertToString(inputType);
                assertThat(requestMap.get("input_type"), is(convertedInputType));
            }
        }
    }

    public void testChatCompletionRequestAction() throws IOException {
        final var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        final var serviceComponents = getServiceComponents();

        try (var sender = createSender(senderFactory)) {
            sender.start();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testCompletionTokenResponseJson));
            final var webserverUrl = getUrl(webServer);
            final var model = AzureAiStudioChatCompletionModelTests.createModel(
                "id",
                "http://will-be-replaced.local",
                AzureAiStudioProvider.COHERE,
                AzureAiStudioEndpointType.TOKEN,
                "apikey"
            );
            model.setURI(webserverUrl);

            final var creator = new AzureAiStudioActionCreator(sender, serviceComponents);
            final var action = creator.create(model, Map.of());

            final PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(new ChatCompletionInput(List.of("abc")), InferenceAction.Request.DEFAULT_TIMEOUT, listener);

            final var result = listener.actionGet(TIMEOUT);

            assertThat(result.asMap(), is(buildExpectationCompletion(List.of("test input string"))));

            assertWebServerRequest(HttpHeaders.AUTHORIZATION, "apikey");

            final MockRequest request = webServer.requests().get(0);
            final var requestMap = entityAsMap(request.getBody());
            assertThat(requestMap.size(), is(1));
            assertThat(requestMap.get("messages"), is(List.of(Map.of("role", "user", "content", "abc"))));
        }
    }

    public void testRerankRequestAction() throws IOException {
        final var senderFactory = HttpRequestSenderTests.createSenderFactory(threadPool, clientManager);
        final var serviceComponents = getServiceComponents();

        try (var sender = createSender(senderFactory)) {
            sender.start();

            webServer.enqueue(new MockResponse().setResponseCode(200).setBody(testRerankTokenResponseJson));
            final var webserverUrl = getUrl(webServer);
            final var model = AzureAiStudioRerankModelTests.createModel(
                "id",
                "http://will-be-replaced.local",
                AzureAiStudioProvider.COHERE,
                AzureAiStudioEndpointType.TOKEN,
                "apikey"
            );
            model.setURI(webserverUrl);

            final var topN = 2;
            final var returnDocuments = false;
            final var query = "query";
            final var documents = List.of("document 1", "document 2", "document 3");

            final var creator = new AzureAiStudioActionCreator(sender, serviceComponents);
            final var action = creator.create(model, Map.of());

            final PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            action.execute(
                new QueryAndDocsInputs(query, documents, returnDocuments, topN, false),
                InferenceAction.Request.DEFAULT_TIMEOUT,
                listener
            );

            final var result = listener.actionGet(TIMEOUT);

            assertThat(
                result.asMap(),
                equalTo(
                    RankedDocsResultsTests.buildExpectationRerank(
                        List.of(
                            new RankedDocsResultsTests.RerankExpectation(Map.of("index", 0, "relevance_score", 0.1111111f)),
                            new RankedDocsResultsTests.RerankExpectation(Map.of("index", 1, "relevance_score", 0.2222222f))
                        )
                    )
                )
            );

            assertWebServerRequest(HttpHeaders.AUTHORIZATION, "apikey");

            final var requestMap = entityAsMap(webServer.requests().get(0).getBody());

            assertThat(requestMap.size(), is(4));
            assertThat(requestMap.get("documents"), is(documents));
            assertThat(requestMap.get("query"), is(query));
            assertThat(requestMap.get("top_n"), is(topN));
            assertThat(requestMap.get("return_documents"), is(returnDocuments));
        }
    }

    private void assertWebServerRequest(String authorization, String authorizationHeaderValue) {
        assertThat(webServer.requests(), hasSize(1));
        assertNull(webServer.requests().get(0).getUri().getQuery());
        assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        assertThat(webServer.requests().get(0).getHeader(authorization), equalTo(authorizationHeaderValue));
    }

    private ServiceComponents getServiceComponents() {
        final var timeoutSettings = buildSettingsWithRetryFields(
            TimeValue.timeValueMillis(1),
            TimeValue.timeValueMinutes(1),
            TimeValue.timeValueSeconds(0)
        );
        return new ServiceComponents(threadPool, mock(ThrottlerManager.class), timeoutSettings, TruncatorTests.createTruncator());
    }

    private final String testEmbeddingsTokenResponseJson = """
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

    private final String testCompletionTokenResponseJson = """
        {
            "choices": [
                {
                    "finish_reason": "stop",
                    "index": 0,
                    "message": {
                        "content": "test input string",
                        "role": "assistant",
                        "tool_calls": null
                    }
                }
            ],
            "created": 1714006424,
            "id": "f92b5b4d-0de3-4152-a3c6-5aae8a74555c",
            "model": "",
            "object": "chat.completion",
            "usage": {
                "completion_tokens": 35,
                "prompt_tokens": 8,
                "total_tokens": 43
            }
        }""";

    private final String testRerankTokenResponseJson = """
        {
            "id": "ff2feb42-5d3a-45d7-ba29-c3dabf59988b",
            "results": [
                {
                    "index": 0,
                    "relevance_score": 0.1111111
                },
                {
                    "index": 1,
                    "relevance_score": 0.2222222
                }
            ],
            "meta": {
                "api_version": {
                    "version": "1"
                },
                "billed_units": {
                    "search_units": 1
                }
            }
        }
        """;
}
