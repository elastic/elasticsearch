/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpClientManager;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.Utils.mockClusterServiceEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HttpRequestSenderFactoryTests extends ESTestCase {
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

            int responseCode = randomIntBetween(200, 203);
            String body = randomAlphaOfLengthBetween(2, 8096);
            webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

            String paramKey = randomAlphaOfLength(3);
            String paramValue = randomAlphaOfLength(3);
            var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            sender.send(httpPost, null, listener);

            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.getURI().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testHttpRequestSender_Throws_WhenCallingSendBeforeStart() throws Exception {
        var senderFactory = new HttpRequestSenderFactory(threadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            var thrownException = expectThrows(AssertionError.class, () -> sender.send(mock(HttpRequestBase.class), listener));
            assertThat(thrownException.getMessage(), is("call start() before sending a request"));
        }
    }

    public void testHttpRequestSender_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new HttpRequestSenderFactory(threadPool, mockManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            assertThat(sender, instanceOf(HttpRequestSenderFactory.HttpRequestSender.class));
            // hack to get around the sender interface so we can set the timeout directly
            var httpSender = (HttpRequestSenderFactory.HttpRequestSender) sender;
            httpSender.setMaxRequestTimeout(TimeValue.timeValueNanos(1));
            sender.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            sender.send(mock(HttpRequestBase.class), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    public void testHttpRequestSenderWithTimeout_Throws_WhenATimeoutOccurs() throws Exception {
        var mockManager = mock(HttpClientManager.class);
        when(mockManager.getHttpClient()).thenReturn(mock(HttpClient.class));

        var senderFactory = new HttpRequestSenderFactory(threadPool, mockManager, mockClusterServiceEmpty(), Settings.EMPTY);

        try (var sender = senderFactory.createSender("test_service")) {
            sender.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            sender.send(mock(HttpRequestBase.class), TimeValue.timeValueNanos(1), listener);

            var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

            assertThat(
                thrownException.getMessage(),
                is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
            );
        }
    }

    private static HttpRequestSenderFactory createSenderFactory(HttpClientManager clientManager, AtomicReference<Thread> threadRef) {
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

        return new HttpRequestSenderFactory(mockThreadPool, clientManager, mockClusterServiceEmpty(), Settings.EMPTY);
    }
}
