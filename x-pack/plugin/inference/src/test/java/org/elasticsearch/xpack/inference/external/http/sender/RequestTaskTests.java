/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.http.MockWebServer;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.logging.ThrottlerManager;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createConnectionManager;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.emptyHttpSettings;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RequestTaskTests extends ESTestCase {

    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private final MockWebServer webServer = new MockWebServer();
    private ThreadPool threadPool;

    @Before
    public void init() throws Exception {
        webServer.start();
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
        webServer.close();
    }

    public void testDoRun_SendsRequestAndReceivesResponse() throws Exception {
        int responseCode = randomIntBetween(200, 203);
        String body = randomAlphaOfLengthBetween(2, 8096);
        webServer.enqueue(new MockResponse().setResponseCode(responseCode).setBody(body));

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        try (var httpClient = HttpClient.create(emptyHttpSettings(), threadPool, createConnectionManager(), mock(ThrottlerManager.class))) {
            httpClient.start();

            PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
            var requestTask = new RequestTask(httpPost, httpClient, HttpClientContext.create(), null, threadPool, listener);
            requestTask.doRun();
            var result = listener.actionGet(TIMEOUT);

            assertThat(result.response().getStatusLine().getStatusCode(), equalTo(responseCode));
            assertThat(new String(result.body(), StandardCharsets.UTF_8), is(body));
            assertThat(webServer.requests(), hasSize(1));
            assertThat(webServer.requests().get(0).getUri().getPath(), equalTo(httpPost.getURI().getPath()));
            assertThat(webServer.requests().get(0).getUri().getQuery(), equalTo(paramKey + "=" + paramValue));
            assertThat(webServer.requests().get(0).getHeader(HttpHeaders.CONTENT_TYPE), equalTo(XContentType.JSON.mediaType()));
        }
    }

    public void testDoRun_SendThrowsIOException() throws Exception {
        var httpClient = mock(HttpClient.class);
        doThrow(new IOException("exception")).when(httpClient).send(any(), any(), any());

        String paramKey = randomAlphaOfLength(3);
        String paramValue = randomAlphaOfLength(3);
        var httpPost = createHttpPost(webServer.getPort(), paramKey, paramValue);

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        var requestTask = new RequestTask(httpPost, httpClient, HttpClientContext.create(), null, threadPool, listener);
        requestTask.doRun();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(format("Failed to send request [%s]", httpPost.getRequestLine())));
    }

    public void testRequest_DoesNotCallOnFailureForTimeout_AfterSendThrowsIllegalArgumentException() throws Exception {
        AtomicReference<Runnable> onTimeout = new AtomicReference<>();
        var mockThreadPool = mockThreadPoolForTimeout(onTimeout);

        var httpClient = mock(HttpClient.class);
        doThrow(new IllegalArgumentException("failed")).when(httpClient).send(any(), any(), any());

        var httpPost = createHttpPost(webServer.getPort(), "a", "b");

        @SuppressWarnings("unchecked")
        ActionListener<HttpResult> listener = mock(ActionListener.class);

        var requestTask = new RequestTask(
            httpPost,
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            mockThreadPool,
            listener
        );

        requestTask.doRun();

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(argument.getValue().getMessage(), is(format("Failed to send request [%s]", httpPost.getRequestLine())));
        assertThat(argument.getValue(), instanceOf(ElasticsearchException.class));
        assertThat(argument.getValue().getCause(), instanceOf(IllegalArgumentException.class));

        onTimeout.get().run();
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_ReturnsTimeoutException() {
        var httpClient = mock(HttpClient.class);

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        var requestTask = new RequestTask(
            mock(HttpRequestBase.class),
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );
        requestTask.doRun();

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueMillis(1)))
        );
    }

    public void testRequest_DoesNotCallOnFailureTwiceWhenTimingOut() throws Exception {
        var httpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new ElasticsearchException("failed"));
            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<HttpResult> listener = mock(ActionListener.class);
        var calledOnFailureLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            calledOnFailureLatch.countDown();
            return Void.TYPE;
        }).when(listener).onFailure(any());

        var requestTask = new RequestTask(
            mock(HttpRequestBase.class),
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );

        calledOnFailureLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(
            argument.getValue().getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueMillis(1)))
        );

        requestTask.doRun();
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_DoesNotCallOnResponseAfterTimingOut() throws Exception {
        var httpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            var result = new HttpResult(mock(HttpResponse.class), new byte[0]);
            listener.onResponse(result);
            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<HttpResult> listener = mock(ActionListener.class);
        var calledOnFailureLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            calledOnFailureLatch.countDown();
            return Void.TYPE;
        }).when(listener).onFailure(any());

        var requestTask = new RequestTask(
            mock(HttpRequestBase.class),
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            threadPool,
            listener
        );

        calledOnFailureLatch.await(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(
            argument.getValue().getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueMillis(1)))
        );

        requestTask.doRun();
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_DoesNotCallOnFailureForTimeout_AfterAlreadyCallingOnFailure() throws Exception {
        AtomicReference<Runnable> onTimeout = new AtomicReference<>();
        var mockThreadPool = mockThreadPoolForTimeout(onTimeout);

        var httpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onFailure(new ElasticsearchException("failed"));
            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<HttpResult> listener = mock(ActionListener.class);

        var requestTask = new RequestTask(
            mock(HttpRequestBase.class),
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            mockThreadPool,
            listener
        );

        requestTask.doRun();

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(argument.capture());
        assertThat(argument.getValue().getMessage(), is("failed"));

        onTimeout.get().run();
        verifyNoMoreInteractions(listener);
    }

    public void testRequest_DoesNotCallOnFailureForTimeout_AfterAlreadyCallingOnResponse() throws Exception {
        AtomicReference<Runnable> onTimeout = new AtomicReference<>();
        var mockThreadPool = mockThreadPoolForTimeout(onTimeout);

        var httpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<HttpResult> listener = (ActionListener<HttpResult>) invocation.getArguments()[2];
            listener.onResponse(new HttpResult(mock(HttpResponse.class), new byte[0]));
            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        @SuppressWarnings("unchecked")
        ActionListener<HttpResult> listener = mock(ActionListener.class);

        var requestTask = new RequestTask(
            mock(HttpRequestBase.class),
            httpClient,
            HttpClientContext.create(),
            TimeValue.timeValueMillis(1),
            mockThreadPool,
            listener
        );

        requestTask.doRun();

        verify(listener, times(1)).onResponse(any());

        onTimeout.get().run();
        verifyNoMoreInteractions(listener);
    }

    private ThreadPool mockThreadPoolForTimeout(AtomicReference<Runnable> onTimeoutRunnable) {
        var mockThreadPool = mock(ThreadPool.class);
        when(mockThreadPool.executor(any())).thenReturn(mock(ExecutorService.class));
        when(mockThreadPool.getThreadContext()).thenReturn(threadPool.getThreadContext());

        doAnswer(invocation -> {
            Runnable runnable = (Runnable) invocation.getArguments()[0];
            onTimeoutRunnable.set(runnable);
            return mock(Scheduler.ScheduledCancellable.class);
        }).when(mockThreadPool).schedule(any(Runnable.class), any(), any());

        return mockThreadPool;
    }
}
