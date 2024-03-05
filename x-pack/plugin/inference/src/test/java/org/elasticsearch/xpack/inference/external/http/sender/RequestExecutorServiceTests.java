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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.Utils.inferenceUtilityPool;
import static org.elasticsearch.xpack.inference.common.AdjustableCapacityBlockingQueueTests.mockQueueCreator;
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
        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), null, new PlainActionFuture<>());

        assertThat(service.queueSize(), is(1));
    }

    public void testIsTerminated_IsFalse() {
        var service = createRequestExecutorServiceWithMocks();

        assertFalse(service.isTerminated());
    }

    public void testIsTerminated_IsTrue() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var service = createRequestExecutorService(latch, mock(RetryingHttpSender.class));

        service.shutdown();
        service.start();
        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertTrue(service.isTerminated());
    }

    public void testIsTerminated_AfterStopFromSeparateThread() throws Exception {
        var waitToShutdown = new CountDownLatch(1);
        var waitToReturnFromSend = new CountDownLatch(1);

        var requestSender = mock(RetryingHttpSender.class);
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            waitToReturnFromSend.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        var service = createRequestExecutorService(null, requestSender);

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, waitToReturnFromSend, service);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(
            OpenAiEmbeddingsExecutableRequestCreatorTests.makeCreator("url", null, "key", "id", null),
            List.of(),
            null,
            listener
        );

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

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to enqueue task because the http executor service [test_service] has already shutdown")
        );
        assertTrue(thrownException.isExecutorShutdown());
    }

    public void testSend_Throws_WhenQueueIsFull() {
        var service = new RequestExecutorService(
            "test_service",
            threadPool,
            null,
            RequestExecutorServiceSettingsTests.createRequestExecutorServiceSettings(1),
            new SingleRequestManager(mock(RetryingHttpSender.class))
        );

        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), null, new PlainActionFuture<>());
        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );
        assertFalse(thrownException.isExecutorShutdown());
    }

    public void testTaskThrowsError_CallsOnFailure() {
        var requestSender = mock(RetryingHttpSender.class);

        var service = createRequestExecutorService(null, requestSender);

        doAnswer(invocation -> {
            service.shutdown();
            throw new IllegalArgumentException("failed");
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();

        service.execute(
            OpenAiEmbeddingsExecutableRequestCreatorTests.makeCreator("url", null, "key", "id", null),
            List.of(),
            null,
            listener
        );
        service.start();

        var thrownException = expectThrows(ElasticsearchException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(thrownException.getMessage(), is(format("Failed to send request from inference entity id [%s]", "id")));
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

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
        );
    }

    public void testSend_NotifiesTasksOfShutdown() {
        var service = createRequestExecutorServiceWithMocks();

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(ExecutableRequestCreatorTests.createMock(), List.of(), null, listener);

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
        BlockingQueue<RejectableTask> queue = mock(LinkedBlockingQueue.class);

        var service = new RequestExecutorService(
            getTestName(),
            threadPool,
            mockQueueCreator(queue),
            null,
            createRequestExecutorServiceSettingsEmpty(),
            new SingleRequestManager(mock(RetryingHttpSender.class))
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
        BlockingQueue<RejectableTask> queue = mock(LinkedBlockingQueue.class);
        when(queue.take()).thenThrow(new InterruptedException("failed"));

        var service = new RequestExecutorService(
            getTestName(),
            threadPool,
            mockQueueCreator(queue),
            null,
            createRequestExecutorServiceSettingsEmpty(),
            new SingleRequestManager(mock(RetryingHttpSender.class))
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

    public void testQueueTake_RejectsTask_WhenServiceShutsDown() throws Exception {
        var mockTask = mock(RejectableTask.class);
        @SuppressWarnings("unchecked")
        BlockingQueue<RejectableTask> queue = mock(LinkedBlockingQueue.class);

        var service = new RequestExecutorService(
            "test_service",
            threadPool,
            mockQueueCreator(queue),
            null,
            createRequestExecutorServiceSettingsEmpty(),
            new SingleRequestManager(mock(RetryingHttpSender.class))
        );

        doAnswer(invocation -> {
            service.shutdown();
            return mockTask;
        }).doReturn(new NoopTask()).when(queue).take();

        service.start();

        assertTrue(service.isTerminated());
        verify(queue, times(1)).take();

        ArgumentCaptor<Exception> argument = ArgumentCaptor.forClass(Exception.class);
        verify(mockTask, times(1)).onRejection(argument.capture());
        assertThat(argument.getValue(), instanceOf(EsRejectedExecutionException.class));
        assertThat(
            argument.getValue().getMessage(),
            is("Failed to send request, queue service [test_service] has shutdown prior to executing request")
        );

        var rejectionException = (EsRejectedExecutionException) argument.getValue();
        assertTrue(rejectionException.isExecutorShutdown());
    }

    public void testChangingCapacity_SetsCapacityToTwo() throws ExecutionException, InterruptedException, TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService("test_service", threadPool, null, settings, new SingleRequestManager(requestSender));

        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, new PlainActionFuture<>());
        assertThat(service.queueSize(), is(1));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );

        settings.setQueueCapacity(2);

        var waitToShutdown = new CountDownLatch(1);
        var waitToReturnFromSend = new CountDownLatch(1);
        // There is a request already queued, and its execution path will initiate shutting down the service
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            waitToReturnFromSend.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, waitToReturnFromSend, service);

        service.start();

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());
        assertThat(service.remainingQueueCapacity(), is(2));
    }

    public void testChangingCapacity_DoesNotRejectsOverflowTasks_BecauseOfQueueFull() throws ExecutionException, InterruptedException,
        TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(3);
        var service = new RequestExecutorService("test_service", threadPool, null, settings, new SingleRequestManager(requestSender));

        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, new PlainActionFuture<>());
        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, new PlainActionFuture<>());

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, listener);
        assertThat(service.queueSize(), is(3));

        settings.setQueueCapacity(1);

        var waitToShutdown = new CountDownLatch(1);
        var waitToReturnFromSend = new CountDownLatch(1);
        // There is a request already queued, and its execution path will initiate shutting down the service
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            waitToReturnFromSend.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, waitToReturnFromSend, service);

        service.start();

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());
        assertThat(service.remainingQueueCapacity(), is(1));
        assertThat(service.queueSize(), is(0));

        var thrownException = expectThrows(
            EsRejectedExecutionException.class,
            () -> listener.actionGet(TIMEOUT.getSeconds(), TimeUnit.SECONDS)
        );
        assertThat(
            thrownException.getMessage(),
            is("Failed to send request, queue service [test_service] has shutdown prior to executing request")
        );
        assertTrue(thrownException.isExecutorShutdown());
    }

    public void testChangingCapacity_ToZero_SetsQueueCapacityToUnbounded() throws IOException, ExecutionException, InterruptedException,
        TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService("test_service", threadPool, null, settings, new SingleRequestManager(requestSender));

        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, new PlainActionFuture<>());
        assertThat(service.queueSize(), is(1));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(ExecutableRequestCreatorTests.createMock(requestSender), List.of(), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is("Failed to execute task because the http executor service [test_service] queue is full")
        );

        settings.setQueueCapacity(0);

        var waitToShutdown = new CountDownLatch(1);
        var waitToReturnFromSend = new CountDownLatch(1);
        // There is a request already queued, and its execution path will initiate shutting down the service
        doAnswer(invocation -> {
            waitToShutdown.countDown();
            waitToReturnFromSend.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, waitToReturnFromSend, service);

        service.start();

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());
        assertThat(service.remainingQueueCapacity(), is(Integer.MAX_VALUE));
    }

    private Future<?> submitShutdownRequest(
        CountDownLatch waitToShutdown,
        CountDownLatch waitToReturnFromSend,
        RequestExecutorService service
    ) {
        return threadPool.generic().submit(() -> {
            try {
                // wait for a task to be added to be executed before beginning shutdown
                waitToShutdown.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
                service.shutdown();
                // tells send to return
                waitToReturnFromSend.countDown();
                service.awaitTermination(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            } catch (Exception e) {
                fail(Strings.format("Failed to shutdown executor: %s", e));
            }
        });
    }

    private RequestExecutorService createRequestExecutorServiceWithMocks() {
        return createRequestExecutorService(null, mock(RetryingHttpSender.class));
    }

    private RequestExecutorService createRequestExecutorService(@Nullable CountDownLatch startupLatch, RetryingHttpSender requestSender) {
        return new RequestExecutorService(
            "test_service",
            threadPool,
            startupLatch,
            createRequestExecutorServiceSettingsEmpty(),
            new SingleRequestManager(requestSender)
        );
    }
}
