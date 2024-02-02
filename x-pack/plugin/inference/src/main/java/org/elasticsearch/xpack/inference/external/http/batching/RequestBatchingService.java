/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.batching;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.AdjustableCapacityBlockingQueue;
import org.elasticsearch.xpack.inference.external.http.RequestExecutor;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

import static org.elasticsearch.core.Strings.format;

/**
 * A service for queuing and executing inference requests. This class attempts to batch the requests. Queuing is necessary because the
 * {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager} will block when no connections are available. To avoid
 * blocking the inference transport threads, this service will queue up the requests until connections are available.
 *
 * <b>NOTE:</b> It is the responsibility of the class constructing the
 * {@link org.apache.http.client.methods.HttpUriRequest} to set a timeout for how long this executor will wait
 * attempting to execute a task (aka waiting for the connection manager to lease a connection). See
 * {@link org.apache.http.client.config.RequestConfig.Builder#setConnectionRequestTimeout} for more info.
 *
 * @param <K> a grouping key for requests, this will usually be an account like class that represents the unique values for a particular
 *           user's third party service's account
 */
public class RequestBatchingService<K> implements RequestExecutor<K> {
    private static final Logger logger = LogManager.getLogger(RequestBatchingService.class);

    private final String serviceName;
    private final AdjustableCapacityBlockingQueue<Task<K>> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final HttpClientContext httpContext;
    private final ThreadPool threadPool;
    private final CountDownLatch startupLatch;
    private final RequestBatcherFactory<K> batcherFactory;
    private final List<Task<K>> taskBuffer;
    private final RequestBatchingServiceSettings settings;
    private final BlockingQueue<Runnable> controlQueue = new LinkedBlockingQueue<>();
    private Instant executeBatchDeadline;

    public RequestBatchingService(
        String serviceName,
        ThreadPool threadPool,
        @Nullable CountDownLatch startupLatch,
        RequestBatcherFactory<K> batcherFactory,
        RequestBatchingServiceSettings settings
    ) {
        this(
            serviceName,
            threadPool,
            buildQueue(settings.getQueueCapacity()),
            RequestBatchingService::buildQueue,
            startupLatch,
            batcherFactory,
            settings
        );
    }

    private static <K> BlockingQueue<Task<K>> buildQueue(int capacity) {
        BlockingQueue<Task<K>> queue;
        if (capacity <= 0) {
            queue = new LinkedBlockingQueue<>();
        } else {
            queue = new LinkedBlockingQueue<>(capacity);
        }

        return queue;
    }

    /**
     * This constructor should only be used directly for testing.
     */
    RequestBatchingService(
        String serviceName,
        ThreadPool threadPool,
        BlockingQueue<Task<K>> queue,
        Function<Integer, BlockingQueue<Task<K>>> createQueue,
        @Nullable CountDownLatch startupLatch,
        RequestBatcherFactory<K> batcherFactory,
        RequestBatchingServiceSettings settings
    ) {
        this.serviceName = Objects.requireNonNull(serviceName);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.batcherFactory = Objects.requireNonNull(batcherFactory);
        this.httpContext = HttpClientContext.create();
        this.queue = new AdjustableCapacityBlockingQueue<>(Objects.requireNonNull(queue), createQueue);
        this.settings = Objects.requireNonNull(settings);
        this.settings.registerQueueCapacityCallback(this::onCapacityChange);
        this.startupLatch = startupLatch;
        taskBuffer = new ArrayList<>(this.settings.getBatchExecutionThreshold());
    }

    private void onCapacityChange(int capacity) {
        var enqueuedCapacityCommand = controlQueue.offer(() -> updateCapacity(capacity));
        if (enqueuedCapacityCommand == false) {
            logger.warn("Failed to change request batching service queue capacity. Control queue was full, please try again later.");
        } else {
            // ensure that the task execution loop wakes up
            queue.offer(new NoopTask<>());
        }
    }

    private void updateCapacity(int newCapacity) {
        try {
            var remainingTasks = queue.setCapacity(newCapacity);
            if (remainingTasks.isEmpty() == false) {
                rejectTasks(remainingTasks);
            }
        } catch (Exception e) {
            logger.warn(
                format("Failed to set the capacity of the task queue to [%s] for request batching service [%s]", newCapacity, serviceName),
                e
            );
        }
    }

    /**
     * Begin servicing tasks.
     */
    public void start() {
        try {
            signalStartInitiated();

            while (running.get()) {
                handleTasks();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            running.set(false);
            notifyRequestsOfShutdown();
            terminationLatch.countDown();
        }
    }

    private void signalStartInitiated() {
        if (startupLatch != null) {
            startupLatch.countDown();
        }
    }

    /**
     * Protects the task retrieval logic from an unexpected exception.
     *
     * @throws InterruptedException rethrows the exception if it occurred retrieving a task because the thread is likely attempting to
     * shut down
     */
    private void handleTasks() throws InterruptedException {
        try {
            var task = queue.poll(settings.getBatchingWaitPeriod().millis(), TimeUnit.MILLISECONDS);

            var command = controlQueue.poll();
            if (command != null) {
                command.run();
            }

            if (running.get() == false) {
                // TODO improve the shutdown logic to run anything that is buffered so far
                logger.debug(this::getServiceExitingMessage);
                return;
            }

            if (task == null) {
                executeBufferedTasks();
            } else {
                bufferTasks(task);
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            logger.warn(format("Request service [%s] failed while retrieving task for execution", serviceName), e);
        }
    }

    private String getServiceExitingMessage() {
        return format("Request service [%s] exiting", serviceName);
    }

    private void executeBufferedTasks() {
        try {
            queue.drainTo(taskBuffer, settings.getBatchExecutionThreshold() - taskBuffer.size());

            if (hasBufferedEnoughTasks() == false && hasReachedBatchingDeadline() == false) {
                return;
            }

            executeBatchDeadline = null;

            var batcher = batcherFactory.create(httpContext);

            batcher.add(taskBuffer);
            taskBuffer.clear();

            runBatch(batcher);
        } catch (Exception e) {
            logger.warn(format("Request service [%s] failed to execute batch request", serviceName), e);
        }
    }

    private boolean hasBufferedEnoughTasks() {
        return taskBuffer.size() >= settings.getBatchExecutionThreshold();
    }

    private boolean hasReachedBatchingDeadline() {
        if (executeBatchDeadline == null) {
            return false;
        }

        return Instant.now().isAfter(executeBatchDeadline);
    }

    private static <K> void runBatch(RequestBatcher<K> batcher) {
        for (var runnable : batcher) {
            runnable.run();
        }
    }

    private void bufferTasks(Task<K> initialTask) {
        try {
            taskBuffer.add(initialTask);

            if (executeBatchDeadline == null) {
                executeBatchDeadline = createBatchExecutionDeadline(settings.getBatchingWaitPeriod());
            }

            executeBufferedTasks();
        } catch (Exception e) {
            logger.warn(format("Request service [%s] failed to execute batch request", serviceName), e);
        }
    }

    private static Instant createBatchExecutionDeadline(TimeValue executionFrequency) {
        return Instant.now().plus(Duration.ofMillis(executionFrequency.millis()));
    }

    private synchronized void notifyRequestsOfShutdown() {
        assert isShutdown() : "Requests should only be notified if the request service is shutting down";

        try {
            List<Task<K>> notExecuted = new ArrayList<>(taskBuffer.size() + queue.size());
            notExecuted.addAll(taskBuffer);
            queue.drainTo(notExecuted);
            taskBuffer.clear();

            rejectTasks(notExecuted);
        } catch (Exception e) {
            logger.warn(format("Failed to notify tasks of request service [%s] shutdown", serviceName));
        }
    }

    private void rejectTasks(List<Task<K>> tasks) {
        for (var task : tasks) {
            rejectTask(task);
        }
    }

    private void rejectTask(Task<K> task) {
        try {
            task.onRejection(
                new EsRejectedExecutionException(
                    format("Failed to send request, request service [%s] has shutdown prior to executing request", serviceName),
                    true
                )
            );
        } catch (Exception e) {
            logger.warn(
                format("Failed to notify request [%s] for service [%s] of rejection after request service shutdown", task, serviceName)
            );
        }
    }

    public int queueSize() {
        return queue.size();
    }

    // default for testing
    int remainingQueueCapacity() {
        return queue.remainingCapacity();
    }

    /**
     * Begins shutting down the service. Some tasks may still execute while the service is shutting down.
     */
    @Override
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            // if this fails because the queue is full, that's ok, we just want to ensure that queue poll returns
            // queue.offer(new ShutdownTask<>());
            queue.offer(new NoopTask<>());
        }
    }

    /**
     * Indicates whether the service is shutdown. If this returns true, no more tasks will be executed.
     * The service still might be in the process of rejecting any previously submitted tasks.
     * @return true if the service has shutdown
     */
    @Override
    public boolean isShutdown() {
        return running.get() == false;
    }

    /**
     * Indicates whether the service has completed rejecting tasks and has fully exited.
     * @return true if the service has fully exited
     */
    @Override
    public boolean isTerminated() {
        return terminationLatch.getCount() == 0;
    }

    /**
     * Blocks until the service has exited or a timeout is reached.
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return true if the service terminated and false if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    /**
     * Submits an inference task to be executed in the future.
     * @param requestCreator handles combining multiple requests into one when they match the creators batching criteria.
     * @param input the text to be sent
     * @param timeout the maximum time this task will wait for a response from the upstream destination before timing out
     * @param listener response channel
     */
    public void submit(
        RequestCreator<K> requestCreator,
        List<String> input,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var preservingListener = ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext());
        RequestTask<K> task = new RequestTask<>(requestCreator, input, timeout, threadPool, preservingListener);

        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to enqueue task because the request service [%s] has already shutdown", serviceName),
                true
            );

            task.onRejection(rejected);
            return;
        }

        boolean added = queue.offer(task);
        if (added == false) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to execute task because the request service [%s] queue is full", serviceName),
                false
            );

            task.onRejection(rejected);
        } else if (isShutdown()) {
            // It is possible that a shutdown and notification request occurred after we initially checked for shutdown above
            // If the task was added after the queue was already drained it could sit there indefinitely. So let's check again if
            // we shut down and if so we'll redo the notification
            notifyRequestsOfShutdown();
        }
    }
}
