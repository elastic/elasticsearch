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
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.batching.BatchingComponents;
import org.elasticsearch.xpack.inference.external.http.batching.OpenAiEmbeddingsRequestCreator;
import org.elasticsearch.xpack.inference.external.http.batching.OpenAiInferenceRequestCreatorTests;
import org.elasticsearch.xpack.inference.external.http.batching.OpenAiRequestBatcherFactory;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.openai.OpenAiAccount;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsModelTests;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
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
        var service = createWithoutLatch();

        assertThat(service.queueSize(), is(0));
    }

    public void testQueueSize_IsOne() {
        var service = createWithoutLatch();
        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), null, new PlainActionFuture<>());

        assertThat(service.queueSize(), is(1));
    }

    public void testExecute_ThrowsUnsupported() {
        var service = createWithoutLatch();
        Runnable noopTask = () -> {};

        var thrownException = expectThrows(UnsupportedOperationException.class, () -> service.execute(noopTask));
        assertThat(thrownException.getMessage(), is("use send instead"));
    }

    public void testIsTerminated_IsFalse() {
        var service = createWithoutLatch();

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            latch,
            new OpenAiRequestBatcherFactory(new BatchingComponents(mock(RequestSender.class), threadPool))
        );

        service.shutdown();
        service.start();
        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertTrue(service.isTerminated());
    }

    public void testIsTerminated_AfterStopFromSeparateThread() {
        var waitToShutdown = new CountDownLatch(1);
        var requestSender = mock(RequestSender.class);

        doAnswer(invocation -> {
            waitToShutdown.countDown();
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any());

        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(requestSender, threadPool))
        );

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

        var model = OpenAiEmbeddingsModelTests.createModel("url", null, "secret", "model", null);
        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.send(OpenAiInferenceRequestCreatorTests.create(model), List.of("abc"), null, listener);

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
        var service = createWithoutLatch();

        service.shutdown();

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Failed to enqueue task because the http executor service [%s] has already shutdown", getTestName()))
        );
    }

    public void testSend_Throws_WhenQueueIsFull() {
        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            1,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(mock(RequestSender.class), threadPool))
        );

        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), null, new PlainActionFuture<>());
        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Failed to execute task because the http executor service [%s] queue is full", getTestName()))
        );
    }

    public void testTaskThrowsError_CallsOnFailure() {
        var requestSender = mock(RequestSender.class);

        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(requestSender, threadPool))
        );

        doAnswer(invocation -> {
            service.shutdown();
            throw new IllegalArgumentException("failed");
        }).when(requestSender).send(any(), any(), any(), any(), any());

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

        var model = OpenAiEmbeddingsModelTests.createModel("url", null, "secret", "model", null);
        service.send(OpenAiInferenceRequestCreatorTests.create(model), List.of("abc"), null, listener);
        service.start();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is("Failed to send request [POST url HTTP/1.1]"));
        assertThat(thrownException.getCause(), instanceOf(IllegalArgumentException.class));
        assertTrue(service.isTerminated());
    }

    public void testShutdown_AllowsMultipleCalls() {
        var service = createWithoutLatch();

        service.shutdown();
        service.shutdown();
        service.start();

        assertTrue(service.isTerminated());
        assertTrue(service.isShutdown());
    }

    public void testSend_CallsOnFailure_WhenRequestTimesOut() {
        var service = createWithoutLatch();

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
        );
    }

    public void testSend_NotifiesTasksOfShutdown() {
        var service = createWithoutLatch();

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.send(mock(OpenAiEmbeddingsRequestCreator.class), List.of("abc"), null, listener);
        service.shutdown();
        service.start();

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(Strings.format("Failed to send request, queue service [%s] has shutdown prior to executing request", getTestName()))
        );
        assertTrue(thrownException.isExecutorShutdown());
        assertTrue(service.isTerminated());
    }

    public void testQueueTake_Throwing_DoesNotCauseServiceToTerminate() throws InterruptedException {
        @SuppressWarnings("unchecked")
        BlockingQueue<Task<OpenAiAccount>> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new ElasticsearchException("failed")).thenReturn(new ShutdownTask<>());

        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            queue,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(mock(RequestSender.class), threadPool))
        );

        service.start();

        assertTrue(service.isTerminated());
        verify(queue, times(2)).take();
    }

    public void testQueueTake_ThrowingInterruptedException_TerminatesService() throws Exception {
        @SuppressWarnings("unchecked")
        BlockingQueue<Task<OpenAiAccount>> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new InterruptedException("failed"));

        var service = new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            queue,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(mock(RequestSender.class), threadPool))
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

    private HttpRequestExecutorService<OpenAiAccount> createWithoutLatch() {
        return new HttpRequestExecutorService<>(
            getTestName(),
            threadPool,
            null,
            new OpenAiRequestBatcherFactory(new BatchingComponents(mock(RequestSender.class), threadPool))
        );
    }
}
