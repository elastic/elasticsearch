/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.elasticsearch.xpack.inference.services.ServiceComponentsTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;
import static org.elasticsearch.xpack.inference.external.http.Utils.entityAsMap;
import static org.elasticsearch.xpack.inference.external.http.Utils.getUrl;
import static org.elasticsearch.xpack.inference.external.request.openai.OpenAiUtils.ORGANIZATION_HEADER;
import static org.elasticsearch.xpack.inference.results.TextEmbeddingResultsTests.buildExpectation;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpRequestSenderTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;
    private HttpClientManager clientManager;
    private final AtomicReference<Thread> threadRef = new AtomicReference<>();

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
        clientManager = HttpClientManager.create(Settings.EMPTY, threadPool, mockClusterServiceEmpty(), mock(ThrottlerManager.class));
        threadRef.set(null);
    }

    @After
    public void shutdown() throws IOException, InterruptedException {
        if (threadRef.get() != null) {
            threadRef.get().join(TIMEOUT.millis());
        }

        clientManager.close();
        terminate(threadPool);
        webServer.close();
    }

    public void testCreateSender_SendsRequestAndReceivesResponse() throws Exception {
        var senderFactory = createSenderFactory(clientManager, threadRef);

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

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(
                OpenAiEmbeddingsExecutableRequestCreatorTests.makeCreator(getUrl(webServer), null, "key", "model", null),
                new DocumentsOnlyInput(List.of("abc")),
                listener
            );

            var result = listener.actionGet(TIMEOUT);
            assertThat(result.asMap(), is(buildExpectation(List.of(List.of(0.0123F, -0.0123F)))));

            assertThat(webServer.requests(), hasSize(1));
            assertNull(webServer.requests().get(0).getUri().getQuery());
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.AUTHORIZATION), equalTo("Bearer key"));
            assertNull(webServer.requests().get(0).getHeader(ORGANIZATION_HEADER));

            var requestMap = entityAsMap(webServer.requests().get(0).getBody());
            assertThat(requestMap.size(), is(2));
            assertThat(requestMap.get("input"), is(List.of("abc")));
            assertThat(requestMap.get("model"), is("model"));
        }
    }

    public void testHttpRequestSender_Throws_WhenCallingSendBeforeStart() throws Exception {
        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            clientManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service")) {
            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(
                AssertionError.class,
                () -> sender.send(ExecutableRequestCreatorTests.createMock(), new DocumentsOnlyInput(List.of()), listener)
            );
            assertThat(thrownException.getMessage(), is("call start() before sending a request"));
        }
    }

    public void testHttpRequestSender_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service")) {
            assertThat(sender, instanceOf(HttpRequestSender.class));
            // hack to get around the sender interface so we can set the timeout directly
            var httpSender = (HttpRequestSender) sender;
            httpSender.setMaxRequestTimeout(TimeValue.timeValueNanos(1));
            sender.start();

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(
                ExecutableRequestCreatorTests.createMock(),
                new DocumentsOnlyInput(List.of()),
                TimeValue.timeValueNanos(1),
                listener
            );

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    public void testHttpRequestSenderWithTimeout_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            mockManager,
            mockClusterServiceEmpty()
        );

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
            sender.send(
                ExecutableRequestCreatorTests.createMock(),
                new DocumentsOnlyInput(List.of()),
                TimeValue.timeValueNanos(1),
                listener
            );

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    private static HttpRequestSender.Factory createSenderFactory(HttpClientManager clientManager, AtomicReference<Thread> threadRef) {
        var mockExecutorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            threadRef.set(new Thread(runnable));
            threadRef.get().start();

            return Void.TYPE;
        }).when(mockExecutorService).execute(any(Runnable.class));

        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(anyString())).thenReturn(mockExecutorService);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockThreadPool.schedule(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.ScheduledCancellable.class));

        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(mockThreadPool),
            clientManager,
            mockClusterServiceEmpty()
        );
    }

    public static HttpRequestSender.Factory createSenderFactory(ThreadPool threadPool, HttpClientManager httpClientManager) {
        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithEmptySettings(threadPool),
            httpClientManager,
            mockClusterServiceEmpty()
        );
    }

    public static HttpRequestSender.Factory createSenderFactory(
        ThreadPool threadPool,
        HttpClientManager httpClientManager,
        Settings settings
    ) {
        return new HttpRequestSender.Factory(
            ServiceComponentsTests.createWithSettings(threadPool, settings),
            httpClientManager,
            mockClusterServiceEmpty()
        );
    }

    public static Sender createSenderWithSingleRequestManager(HttpRequestSender.Factory factory, String serviceName) {
        return factory.createSender(serviceName);
    }
}
