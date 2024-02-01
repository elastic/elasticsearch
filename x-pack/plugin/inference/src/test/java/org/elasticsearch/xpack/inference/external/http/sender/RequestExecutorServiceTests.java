/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;
import org.elasticsearch.xpack.inference.external.request.HttpRequestTests;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.external.http.HttpClientTests.createHttpPost;
import static org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettingsTests.createRequestExecutorServiceSettings;
import static org.elasticsearch.xpack.inference.external.http.sender.RequestExecutorServiceSettingsTests.createRequestExecutorServiceSettingsEmpty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RequestExecutorServiceTests extends ESTestCase {
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
        var service = createRequestExecutorServiceWithMocks();

        assertThat(service.queueSize(), is(0));
    }

    public void testQueueSize_IsOne() {
        var service = createRequestExecutorServiceWithMocks();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, new PlainActionFuture<>());

        assertThat(service.queueSize(), is(1));
    }

    public void testIsTerminated_IsFalse() {
        var service = createRequestExecutorServiceWithMocks();

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var service = createRequestExecutorService(null, latch);

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

        var service = createRequestExecutorService(mockHttpClient, null);

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, service);

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, listener);

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
        var service = createRequestExecutorServiceWithMocks();

        service.shutdown();

        var listener = new PlainActionFuture<HttpResult>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to enqueue task because the http executor service [test_service] has already shutdown")
        );
    }

    public void testSend_Throws_WhenQueueIsFull() {
        var service = new RequestExecutorService(
            "test_service",
            mock(HttpClient.class),
            threadPool,
            null,
            RequestExecutorServiceSettingsTests.createRequestExecutorServiceSettings(1)
        );

        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, new PlainActionFuture<>());
        var listener = new PlainActionFuture<HttpResult>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );
    }

    public void testTaskThrowsError_CallsOnFailure() throws Exception {
        var httpClient = mock(HttpClient.class);

        var service = createRequestExecutorService(httpClient, null);

        doAnswer(invocation -> {
            service.shutdown();
            throw new IllegalArgumentException("failed");
        }).when(httpClient).send(any(), any(), any());

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();

        var request = createHttpPost(0, "a", "b");
        service.execute(request, null, listener);
        service.start();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is(format("Failed to send request from inference entity id [%s]", request.inferenceEntityId()))
        );
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
        assertTrue(service.isTerminated());
    }

    public void testShutdown_AllowsMultipleCalls() {
        var service = createRequestExecutorServiceWithMocks();

        service.shutdown();
        service.shutdown();
        service.start();

        assertTrue(service.isTerminated());
        assertTrue(service.isShutdown());
    }

    public void testSend_CallsOnFailure_WhenRequestTimesOut() {
        var service = createRequestExecutorServiceWithMocks();

        var listener = new PlainActionFuture<HttpResult>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be executed after [%s]", TimeValue.timeValueNanos(1)))
        );
    }

    public void testSend_NotifiesTasksOfShutdown() {
        var service = createRequestExecutorServiceWithMocks();

        var listener = new PlainActionFuture<HttpResult>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, listener);
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

    public void testQueueTake_DoesNotCauseServiceToTerminate_WhenItThrows() throws InterruptedException {
        @SuppressWarnings("unchecked")
        BlockingQueue<AbstractRunnable> queue = mock(LinkedBlockingQueue.class);

        var service = new RequestExecutorService(
            getTestName(),
            mock(HttpClient.class),
            threadPool,
            queue,
            RequestExecutorServiceTests::createQueue,
            null,
            createRequestExecutorServiceSettingsEmpty()
        );

        when(queue.take()).thenThrow(new ElasticsearchException("failed")).thenAnswer(invocation -> {
            service.shutdown();
            return null;
        });
        service.start();

        assertTrue(service.isTerminated());
        verify(queue, times(2)).take();
    }

    public void testQueueTake_ThrowingInterruptedException_TerminatesService() throws Exception {
        @SuppressWarnings("unchecked")
        BlockingQueue<AbstractRunnable> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new InterruptedException("failed"));

        var service = new RequestExecutorService(
            getTestName(),
            mock(HttpClient.class),
            threadPool,
            queue,
            RequestExecutorServiceTests::createQueue,
            null,
            createRequestExecutorServiceSettingsEmpty()
        );

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

    public void testChangingCapacity_SetsCapacityToTwo() throws ExecutionException, InterruptedException, TimeoutException, IOException {
        var waitToShutdown = new CountDownLatch(1);
        var httpClient = mock(HttpClient.class);

        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService("test_service", httpClient, threadPool, null, settings);

        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, new PlainActionFuture<>());
        assertThat(service.queueSize(), is(1));

        PlainActionFuture<HttpResult> listener = new PlainActionFuture<>();
        service.execute(HttpRequestTests.createMock("inferenceEntityId"), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );

        settings.setQueueCapacity(2);

        // There is a request already queued, and its execution path will initiate shutting down the service
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            return Void.TYPE;
        }).when(httpClient).send(any(), any(), any());

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, service);

        service.start();

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());
        assertThat(service.remainingQueueCapacity(), is(2));
    }

    private Future<?> submitShutdownRequest(CountDownLatch waitToShutdown, RequestExecutorService service) {
        return threadPool.generic().submit(() -> {
            try {
                // wait for a task to be added to be executed before beginning shutdown
                waitToShutdown.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                service.shutdown();
                service.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                fail(Strings.format("Failed to shutdown executor: %s", e));
            }
        });
    }

    private RequestExecutorService createRequestExecutorServiceWithMocks() {
        return createRequestExecutorService(null, null);
    }

    private RequestExecutorService createRequestExecutorService(@Nullable HttpClient httpClient, @Nullable CountDownLatch startupLatch) {
        var httpClientToUse = httpClient == null ? mock(HttpClient.class) : httpClient;
        return new RequestExecutorService(
            "test_service",
            httpClientToUse,
            threadPool,
            startupLatch,
            createRequestExecutorServiceSettingsEmpty()
        );
    }

    private static LinkedBlockingQueue<AbstractRunnable> createQueue(int capacity) {
        return new LinkedBlockingQueue<>(capacity);
    }
}
