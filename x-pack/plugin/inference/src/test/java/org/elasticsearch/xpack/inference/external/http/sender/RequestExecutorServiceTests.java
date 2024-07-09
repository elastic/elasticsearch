/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.RateLimiter;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.external.http.retry.RetryingHttpSender;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
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
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
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
        service.execute(RequestManagerTests.createMock(), new DocumentsOnlyInput(List.of()), null, new PlainActionFuture<>());

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

    public void testCallingStartTwice_ThrowsAssertionException() throws InterruptedException {
        var latch = new CountDownLatch(1);
        var service = createRequestExecutorService(latch, mock(RetryingHttpSender.class));

        service.shutdown();
        service.start();
        latch.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);

        assertTrue(service.isTerminated());
        var exception = expectThrows(AssertionError.class, service::start);
        assertThat(exception.getMessage(), is("start() can only be called once"));
    }

    public void testIsTerminated_AfterStopFromSeparateThread() {
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
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "id", null, threadPool),
            new DocumentsOnlyInput(List.of()),
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

    public void testExecute_AfterShutdown_Throws() {
        var service = createRequestExecutorServiceWithMocks();

        service.shutdown();

        var requestManager = RequestManagerTests.createMock("id");
        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to enqueue task for inference id [id] because the request service [%s] has already shutdown",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
        );
        assertTrue(thrownException.isExecutorShutdown());
    }

    public void testExecute_Throws_WhenQueueIsFull() {
        var service = new RequestExecutorService(threadPool, null, createRequestExecutorServiceSettings(1), mock(RetryingHttpSender.class));

        service.execute(RequestManagerTests.createMock(), new DocumentsOnlyInput(List.of()), null, new PlainActionFuture<>());

        var requestManager = RequestManagerTests.createMock("id");
        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to execute task for inference id [id] because the request service [%s] queue is full",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
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
            OpenAiEmbeddingsRequestManagerTests.makeCreator("url", null, "key", "id", null, threadPool),
            new DocumentsOnlyInput(List.of()),
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

    public void testExecute_CallsOnFailure_WhenRequestTimesOut() {
        var service = createRequestExecutorServiceWithMocks();

        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(RequestManagerTests.createMock(), new DocumentsOnlyInput(List.of()), TimeValue.timeValueNanos(1), listener);

        var thrownException = expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(format("Request timed out waiting to be sent after [%s]", TimeValue.timeValueNanos(1)))
        );
    }

    public void testExecute_PreservesThreadContext() throws InterruptedException, ExecutionException, TimeoutException {
        var headerKey = "not empty";
        var headerValue = "value";

        var service = createRequestExecutorServiceWithMocks();

        // starting this on a separate thread to ensure we aren't using the same thread context that the rest of the test will execute with
        threadPool.generic().execute(service::start);

        ThreadContext threadContext = threadPool.getThreadContext();
        threadContext.putHeader(headerKey, headerValue);

        var requestSender = mock(RetryingHttpSender.class);

        var waitToShutdown = new CountDownLatch(1);
        var waitToReturnFromSend = new CountDownLatch(1);

        // this code will be executed by the queue's thread
        doAnswer(invocation -> {
            var serviceThreadContext = threadPool.getThreadContext();
            // ensure that the spawned thread didn't pick up the header that was set initially on a separate thread
            assertNull(serviceThreadContext.getHeader(headerKey));

            @SuppressWarnings("unchecked")
            ActionListener<InferenceServiceResults> listener = (ActionListener<InferenceServiceResults>) invocation.getArguments()[5];
            listener.onResponse(null);

            waitToShutdown.countDown();
            waitToReturnFromSend.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        var finishedOnResponse = new CountDownLatch(1);
        ActionListener<InferenceServiceResults> listener = new ActionListener<>() {
            @Override
            public void onResponse(InferenceServiceResults ignore) {
                // if we've preserved the thread context correctly then the header should still exist
                ThreadContext listenerContext = threadPool.getThreadContext();
                assertThat(listenerContext.getHeader(headerKey), is(headerValue));
                finishedOnResponse.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                throw new RuntimeException("onFailure shouldn't be called", e);
            }
        };

        service.execute(RequestManagerTests.createMock(requestSender), new DocumentsOnlyInput(List.of()), null, listener);

        Future<?> executorTermination = submitShutdownRequest(waitToShutdown, waitToReturnFromSend, service);

        executorTermination.get(TIMEOUT.millis(), TimeUnit.MILLISECONDS);
        assertTrue(service.isTerminated());

        finishedOnResponse.await(TIMEOUT.getSeconds(), TimeUnit.SECONDS);
    }

    public void testExecute_NotifiesTasksOfShutdown() {
        var service = createRequestExecutorServiceWithMocks();

        var requestManager = RequestManagerTests.createMock(mock(RequestSender.class), "id");
        var listener = new PlainActionFuture<InferenceServiceResults>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        service.shutdown();
        service.start();

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));

        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to send request, request service [%s] for inference id [id] has shutdown prior to executing request",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
        );
        assertTrue(thrownException.isExecutorShutdown());
        assertTrue(service.isTerminated());
    }

    public void testQueuePoll_DoesNotCauseServiceToTerminate_WhenItThrows() throws InterruptedException {
        @SuppressWarnings("unchecked")
        BlockingQueue<RejectableTask> queue = mock(LinkedBlockingQueue.class);

        var requestSender = mock(RetryingHttpSender.class);

        var service = new RequestExecutorService(
            threadPool,
            mockQueueCreator(queue),
            null,
            createRequestExecutorServiceSettingsEmpty(),
            requestSender,
            Clock.systemUTC(),
            RequestExecutorService.DEFAULT_SLEEPER,
            RequestExecutorService.DEFAULT_RATE_LIMIT_CREATOR
        );

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        var requestManager = RequestManagerTests.createMock(requestSender, "id");
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        when(queue.poll()).thenThrow(new ElasticsearchException("failed")).thenAnswer(invocation -> {
            service.shutdown();
            return null;
        });
        service.start();

        assertTrue(service.isTerminated());
    }

    public void testSleep_ThrowingInterruptedException_TerminatesService() throws Exception {
        @SuppressWarnings("unchecked")
        BlockingQueue<RejectableTask> queue = mock(LinkedBlockingQueue.class);
        var sleeper = mock(RequestExecutorService.Sleeper.class);
        doThrow(new InterruptedException("failed")).when(sleeper).sleep(any());

        var service = new RequestExecutorService(
            threadPool,
            mockQueueCreator(queue),
            null,
            createRequestExecutorServiceSettingsEmpty(),
            mock(RetryingHttpSender.class),
            Clock.systemUTC(),
            sleeper,
            RequestExecutorService.DEFAULT_RATE_LIMIT_CREATOR
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
    }

    public void testChangingCapacity_SetsCapacityToTwo() throws ExecutionException, InterruptedException, TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService(threadPool, null, settings, requestSender);

        service.execute(RequestManagerTests.createMock(requestSender), new DocumentsOnlyInput(List.of()), null, new PlainActionFuture<>());
        assertThat(service.queueSize(), is(1));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        var requestManager = RequestManagerTests.createMock(requestSender, "id");
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to execute task for inference id [id] because the request service [%s] queue is full",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
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
        assertThat(service.remainingQueueCapacity(requestManager), is(2));
    }

    public void testChangingCapacity_DoesNotRejectsOverflowTasks_BecauseOfQueueFull() throws ExecutionException, InterruptedException,
        TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(3);
        var service = new RequestExecutorService(threadPool, null, settings, requestSender);

        service.execute(
            RequestManagerTests.createMock(requestSender, "id"),
            new DocumentsOnlyInput(List.of()),
            null,
            new PlainActionFuture<>()
        );
        service.execute(
            RequestManagerTests.createMock(requestSender, "id"),
            new DocumentsOnlyInput(List.of()),
            null,
            new PlainActionFuture<>()
        );

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        var requestManager = RequestManagerTests.createMock(requestSender, "id");
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);
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
        assertThat(service.remainingQueueCapacity(requestManager), is(1));
        assertThat(service.queueSize(), is(0));

        var thrownException = expectThrows(
            EsRejectedExecutionException.class,
            () -> listener.actionGet(TIMEOUT.getSeconds(), TimeUnit.SECONDS)
        );
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to send request, request service [%s] for inference id [id] has shutdown prior to executing request",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
        );
        assertTrue(thrownException.isExecutorShutdown());
    }

    public void testChangingCapacity_ToZero_SetsQueueCapacityToUnbounded() throws IOException, ExecutionException, InterruptedException,
        TimeoutException {
        var requestSender = mock(RetryingHttpSender.class);

        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService(threadPool, null, settings, requestSender);
        var requestManager = RequestManagerTests.createMock(requestSender);

        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, new PlainActionFuture<>());
        assertThat(service.queueSize(), is(1));

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(RequestManagerTests.createMock(requestSender, "id"), new DocumentsOnlyInput(List.of()), null, listener);

        var thrownException = expectThrows(EsRejectedExecutionException.class, () -> listener.actionGet(TIMEOUT));
        assertThat(
            thrownException.getMessage(),
            is(
                Strings.format(
                    "Failed to execute task for inference id [id] because the request service [%s] queue is full",
                    requestManager.rateLimitGrouping().hashCode()
                )
            )
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
        assertThat(service.remainingQueueCapacity(requestManager), is(Integer.MAX_VALUE));
    }

    public void testDoesNotExecuteTask_WhenCannotReserveTokens() {
        var mockRateLimiter = mock(RateLimiter.class);
        RequestExecutorService.RateLimiterCreator rateLimiterCreator = (a, b, c) -> mockRateLimiter;

        var requestSender = mock(RetryingHttpSender.class);
        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService(
            threadPool,
            RequestExecutorService.DEFAULT_QUEUE_CREATOR,
            null,
            settings,
            requestSender,
            Clock.systemUTC(),
            RequestExecutorService.DEFAULT_SLEEPER,
            rateLimiterCreator
        );
        var requestManager = RequestManagerTests.createMock(requestSender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        doAnswer(invocation -> {
            service.shutdown();
            return TimeValue.timeValueDays(1);
        }).when(mockRateLimiter).timeToReserve(anyInt());

        service.start();

        verifyNoInteractions(requestSender);
    }

    public void testDoesNotExecuteTask_WhenCannotReserveTokens_AndThenCanReserve_AndExecutesTask() {
        var mockRateLimiter = mock(RateLimiter.class);
        when(mockRateLimiter.reserve(anyInt())).thenReturn(TimeValue.timeValueDays(0));

        RequestExecutorService.RateLimiterCreator rateLimiterCreator = (a, b, c) -> mockRateLimiter;

        var requestSender = mock(RetryingHttpSender.class);
        var settings = createRequestExecutorServiceSettings(1);
        var service = new RequestExecutorService(
            threadPool,
            RequestExecutorService.DEFAULT_QUEUE_CREATOR,
            null,
            settings,
            requestSender,
            Clock.systemUTC(),
            RequestExecutorService.DEFAULT_SLEEPER,
            rateLimiterCreator
        );
        var requestManager = RequestManagerTests.createMock(requestSender);

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        when(mockRateLimiter.timeToReserve(anyInt())).thenReturn(TimeValue.timeValueDays(1)).thenReturn(TimeValue.timeValueDays(0));

        doAnswer(invocation -> {
            service.shutdown();
            return Void.TYPE;
        }).when(requestSender).send(any(), any(), any(), any(), any(), any());

        service.start();

        verify(requestSender, times(1)).send(any(), any(), any(), any(), any(), any());
    }

    public void testRemovesRateLimitGroup_AfterStaleDuration() {
        var now = Instant.now();
        var clock = mock(Clock.class);
        when(clock.instant()).thenReturn(now);

        var requestSender = mock(RetryingHttpSender.class);
        var settings = createRequestExecutorServiceSettings(2, TimeValue.timeValueDays(1));
        var service = new RequestExecutorService(
            threadPool,
            RequestExecutorService.DEFAULT_QUEUE_CREATOR,
            null,
            settings,
            requestSender,
            clock,
            RequestExecutorService.DEFAULT_SLEEPER,
            RequestExecutorService.DEFAULT_RATE_LIMIT_CREATOR
        );
        var requestManager = RequestManagerTests.createMock(requestSender, "id1");

        PlainActionFuture<InferenceServiceResults> listener = new PlainActionFuture<>();
        service.execute(requestManager, new DocumentsOnlyInput(List.of()), null, listener);

        assertThat(service.numberOfRateLimitGroups(), is(1));
        // the time is moved to after the stale duration, so now we should remove this grouping
        when(clock.instant()).thenReturn(now.plus(Duration.ofDays(2)));
        service.removeStaleGroupings();
        assertThat(service.numberOfRateLimitGroups(), is(0));

        var requestManager2 = RequestManagerTests.createMock(requestSender, "id2");
        service.execute(requestManager2, new DocumentsOnlyInput(List.of()), null, listener);

        assertThat(service.numberOfRateLimitGroups(), is(1));
    }

    public void testStartsCleanupThread() {
        var mockThreadPool = mock(ThreadPool.class);

        when(mockThreadPool.scheduleWithFixedDelay(any(Runnable.class), any(), any())).thenReturn(mock(Scheduler.Cancellable.class));

        var requestSender = mock(RetryingHttpSender.class);
        var settings = createRequestExecutorServiceSettings(2, TimeValue.timeValueDays(1));
        var service = new RequestExecutorService(
            mockThreadPool,
            RequestExecutorService.DEFAULT_QUEUE_CREATOR,
            null,
            settings,
            requestSender,
            Clock.systemUTC(),
            RequestExecutorService.DEFAULT_SLEEPER,
            RequestExecutorService.DEFAULT_RATE_LIMIT_CREATOR
        );

        service.shutdown();
        service.start();

        ArgumentCaptor<TimeValue> argument = ArgumentCaptor.forClass(TimeValue.class);
        verify(mockThreadPool, times(1)).scheduleWithFixedDelay(any(Runnable.class), argument.capture(), any());
        assertThat(argument.getValue(), is(TimeValue.timeValueDays(1)));
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
        return new RequestExecutorService(threadPool, startupLatch, createRequestExecutorServiceSettingsEmpty(), requestSender);
    }
}
