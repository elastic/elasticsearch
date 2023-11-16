/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.openai;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.sender.HttpRequestSenderFactory;
import org.elasticsearch.xpack.inference.external.http.sender.Sender;
import org.elasticsearch.xpack.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.services.ServiceComponents;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.retry.RetrySettingsTests.buildSettingsWithRetryFields;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiEmbeddingsRequestTests.createRequest;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.logging.ThrottlerManagerTests.mockThrottlerManager;
import static org.elasticsearch.xpack.inference.services.ServiceComponentsTests.createWithEmptySettings;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

public class OpenAiClientTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mockThrottlerManager());
    }

    @After
    public void shutdown() throws IOException {
        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testSend_SuccessfulResponse() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
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

            OpenAiClient openAiClient = new OpenAiClient(sender, createWithEmptySettings(threadPool));

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            openAiClient.send(createRequest(getUrl(webServer), "org", "secret", "abc", "model", "user"), listener);

            InferenceResults result = listener.actionGet(TIMEOUT).get(0);

            assertThat(
                result.asMap(),
                is(
                    Map.of(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.0123F, -0.0123F)))
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testSend_SuccessfulResponse_WithoutUser() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
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

            OpenAiClient openAiClient = new OpenAiClient(sender, createWithEmptySettings(threadPool));

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            openAiClient.send(createRequest(getUrl(webServer), "org", "secret", "abc", "model", null), listener);

            InferenceResults result = listener.actionGet(TIMEOUT).get(0);

            assertThat(
                result.asMap(),
                is(
                    Map.of(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.0123F, -0.0123F)))
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testSend_SuccessfulResponse_WithoutOrganization() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
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

            OpenAiClient openAiClient = new OpenAiClient(sender, createWithEmptySettings(threadPool));

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            openAiClient.send(createRequest(getUrl(webServer), null, "secret", "abc", "model", null), listener);

            InferenceResults result = listener.actionGet(TIMEOUT).get(0);

            assertThat(
                result.asMap(),
                is(
                    Map.of(
                        TextEmbeddingResults.TEXT_EMBEDDING,
                        List.of(Map.of(TextEmbeddingResults.Embedding.EMBEDDING, List.of(0.0123F, -0.0123F)))
                    )
                )
            );

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertNull(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testSend_FailsFromInvalidResponseFormat() throws IOException, URISyntaxException {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
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

            OpenAiClient openAiClient = new OpenAiClient(
                sender,
                new ServiceComponents(
                    threadPool,
                    mockThrottlerManager(),
                    // timeout as zero for no retries
                    buildSettingsWithRetryFields(TimeValue.timeValueMillis(1), TimeValue.timeValueMinutes(1), TimeValue.timeValueSeconds(0))
                )
            );

            PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
            openAiClient.send(createRequest(getUrl(webServer), "org", "secret", "abc", "model", "user"), listener);

            var thrownException = expectThrows(IllegalStateException.class, () -> listener.actionGet(TIMEOUT));
            assertThat(thrownException.getMessage(), is(format("Failed to find required field [data] in OpenAI embeddings response")));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer secret"));
            assertThat(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER), equalTo("org"));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(3));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
            assertThat(requestMap.get("user"), is("user"));
        }
    }

    public void testSend_ThrowsException() throws URISyntaxException, IOException {
        var sender = mock(Sender.class);
        doThrow(new ElasticsearchException("failed")).when(sender).send(any(), any());

        OpenAiClient openAiClient = new OpenAiClient(sender, createWithEmptySettings(threadPool));
        PlainActionFuture<List<? extends InferenceResults>> listener = new PlainActionFuture<>();
        openAiClient.send(createRequest(getUrl(webServer), "org", "secret", "abc", "model", "user"), listener);

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
    }
}
