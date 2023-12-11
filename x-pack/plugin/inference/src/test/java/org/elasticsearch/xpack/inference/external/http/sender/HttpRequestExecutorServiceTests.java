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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.external.http.Utils.inferenceUtilityPool;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HttpRequestExecutorServiceTests extends ESTestCase {
    private static final TimeValue TIMEOUT = new TimeValue(30, TimeUnit.SECONDS);
    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = createThreadPool(inferenceUtilityPool());
    }

    @After
    public void shutdown() {
        terminate(threadPool);
    }

    public void testQueueSize_IsEmpty() {
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, null);

        assertThat(service.queueSize(), is(0));
    }

    public void testQueueSize_IsOne() {
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, null);
        service.send(mock(HttpRequestBase.class), null, new PlainActionFuture<>());

        assertThat(service.queueSize(), is(1));
    }

    public void testExecute_ThrowsUnsupported() {
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, null);
        var noopTask = mock(RequestTask.class);

        var thrownException = expectThrows(UnsupportedOperationException.class, () -> service.execute(noopTask));
        assertThat(thrownException.getMessage(), is("use send instead"));
    }

    public void testIsTerminated_IsFalse() {
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, null);

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, latch);

        service.shutdown();
        service.start();
        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertTrue(service.isTerminated());
    }

    public void testIsTerminated_AfterStopFromSeparateThread() throws Exception {
        var waitToShutdown = new CountDownLatch(1);

        var mockHttpClient = mock(HttpClient.class);
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            return Void.TYPE;
        }).when(mockHttpClient).send(any(), any(), any());

        var service = new HttpRequestExecutorService(getTestName(), mockHttpClient, threadPool, null);

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
        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, null);

        service.shutdown();

        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to enqueue task because the http executor service [test_service] has already shutdown")
        );
    }

    public void testSend_Throws_WhenQueueIsFull() {
        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, 1, null);

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

        var service = new HttpRequestExecutorService(getTestName(), httpClient, threadPool, null);

        doAnswer(invocation -> {
            service.shutdown();
            throw new IllegalArgumentException("failed");
        }).when(httpClient).send(any(), any(), any());

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();

        var request = createHttpPost(0, "a", "b");
        service.send(request, null, listener);
        service.start();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(format("Failed to send request [%s]", request.getRequestLine())));
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
        assertTrue(service.isTerminated());
    }

    public void testShutdown_AllowsMultipleCalls() {
        var service = new HttpRequestExecutorService(getTestName(), mock(HttpClient.class), threadPool, null);

        service.shutdown();
        service.shutdown();
        service.shutdownNow();
        service.start();

        assertTrue(service.isTerminated());
        assertTrue(service.isShutdown());
    }

    public void testSend_CallsOnFailure_WhenRequestTimesOut() {
        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, null);

        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
        );
    }

    public void testSend_NotifiesTasksOfShutdown() {
        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, null);

        var listener = new PlainActionFuture<HttpResult>();
        service.send(mock(HttpRequestBase.class), null, listener);
        service.shutdown();
        service.start();

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to send request, queue service [test_service] has shutdown prior to executing request")
        );
        assertTrue(thrownException.isExecutorShutdown());
        assertTrue(service.isTerminated());
    }

    public void testQueueTake_Throwing_DoesNotCauseServiceToTerminate() throws InterruptedException {
        @SuppressWarnings("unchecked")
        BlockingQueue<HttpTask> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new ElasticsearchException("failed")).thenReturn(new ShutdownTask());

        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, queue, null);

        service.start();

        assertTrue(service.isTerminated());
        verify(queue, times(2)).take();
    }

    public void testQueueTake_ThrowingInterruptedException_TerminatesService() throws Exception {
        @SuppressWarnings("unchecked")
        BlockingQueue<HttpTask> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new InterruptedException("failed"));

        var service = new HttpRequestExecutorService("test_service", mock(HttpClient.class), threadPool, queue, null);

        Future<?> executorTermination = threadPool.generic().submit(() -> {
            try {
                service.start();
            } catch (Exception e) {
                fail(Strings.format("Failed to shutdown executor: %s", e));
            }
        });

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);

        assertTrue(service.isTerminated());
        verify(queue, times(1)).take();
    }
}
