/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.methods.HttpRequestBase;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createThreadPool;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class HttpRequestExecutorServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(getTestName());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testQueueSize_IsEmpty() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);

        assertThat(service.queueSize(), is(0));
    }

    public void testQueueSize_IsOne() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);
        service.send(mock(HttpRequestBase.class), null, new PlainActionFuture<>());

        assertThat(service.queueSize(), is(1));
    }

    public void testExecute_ThrowsUnsupported() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);
        var noopTask = mock(RequestTask.class);

        var thrownException = expectThrows(UnsupportedOperationException.class, () -> service.execute(noopTask));
        assertThat(thrownException.getMessage(), is("use send instead"));
    }

    public void testIsTerminated_IsFalse() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);

        service.shutdown();
        service.start();

        assertTrue(service.isTerminated());
    }

    public void testIsTerminated_AfterStopFromSeparateThread() throws Exception {
        var waitToShutdown = new CountDownLatch(1);

        var mockHttpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            return Void.TYPE;
        }).when(mockHttpClient).send(any(), any(), any());

        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mockHttpClient, threadPool);

        Future<?> executorTermination = threadPool.generic().submit(() -> {
            try {
                // wait for a task to be added to be executed before beginning shutdown
                waitToShutdown.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                service.shutdown();
                service.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                fail(Strings.format("Failed to shutdown executor: %s", e));
            }
        });

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        service.send(mock(HttpRequestBase.class), null, listener);

        service.start();

        try {
            executorTermination.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail(Strings.format("Executor finished before it was signaled to shutdown: %s", e));
        }

        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }

    public void testSend_AfterShutdown_Throws() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), "test_service", mock(HttpClient.class), threadPool);

        service.shutdown();

        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] has shutdown")
        );
    }

    public void testSend_Throws_WhenQueueIsFull() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), "test_service", mock(HttpClient.class), threadPool, 1);

        service.send(mock(HttpRequestBase.class), null, new PlainActionFuture<>());
        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );
    }

    public void testTaskThrowsError_CallsOnFailure() throws Exception {
        var httpClient = mock(HttpClient.class);

        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), httpClient, threadPool);

        doAnswer(invocation -> {
            service.shutdown();
            throw new ElasticsearchException("failed");
        }).when(httpClient).send(any(), any(), any());

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();

        service.send(mock(HttpRequestBase.class), null, listener);
        service.start();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("failed"));
        assertTrue(service.isTerminated());
    }

    public void testShutdown_AllowsMultipleCalls() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), getTestName(), mock(HttpClient.class), threadPool);

        service.shutdown();
        service.shutdown();
        service.shutdownNow();
        service.start();

        assertTrue(service.isTerminated());
        assertTrue(service.isShutdown());
    }

    public void testSend_CallsOnFailure_WhenRequestTimesOut() {
        var service = new HttpRequestExecutorService(threadPool.getThreadContext(), "test_service", mock(HttpClient.class), threadPool);

        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
        );
    }
}
