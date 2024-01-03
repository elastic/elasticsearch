/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.core.Strings.format;

/**
 * An {@link java.util.concurrent.ExecutorService} for queuing and executing {@link RequestTask} containing
 * {@link org.apache.http.client.methods.HttpUriRequest}. This class is useful because the
 * {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager} will block when leasing a connection if no
 * connections are available. To avoid blocking the inference transport threads, this executor will queue up the
 * requests until connections are available.
 *
 * <b>NOTE:</b> It is the responsibility of the class constructing the
 * {@link org.apache.http.client.methods.HttpUriRequest} to set a timeout for how long this executor will wait
 * attempting to execute a task (aka waiting for the connection manager to lease a connection). See
 * {@link org.apache.http.client.config.RequestConfig.Builder#setConnectionRequestTimeout} for more info.
 */
class HttpRequestExecutorService implements ExecutorService {
    private static final Logger logger = LogManager.getLogger(HttpRequestExecutorService.class);

    private final String serviceName;
    private final BlockingQueue<HttpTask> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final HttpClientContext httpContext;
    private final HttpClient httpClient;
    private final ThreadPool threadPool;
    private final CountDownLatch startupLatch;

    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(String serviceName, HttpClient httpClient, ThreadPool threadPool, @Nullable CountDownLatch startupLatch) {
        this(serviceName, httpClient, threadPool, new LinkedBlockingQueue<>(), startupLatch);
    }

    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(
        String serviceName,
        HttpClient httpClient,
        ThreadPool threadPool,
        int capacity,
        @Nullable CountDownLatch startupLatch
    ) {
        this(serviceName, httpClient, threadPool, new LinkedBlockingQueue<>(capacity), startupLatch);
    }

    /**
     * This constructor should only be used directly for testing.
     */
    @SuppressForbidden(reason = "wraps a queue and handles errors appropriately")
    HttpRequestExecutorService(
        String serviceName,
        HttpClient httpClient,
        ThreadPool threadPool,
        BlockingQueue<HttpTask> queue,
        @Nullable CountDownLatch startupLatch
    ) {
        this.serviceName = Objects.requireNonNull(serviceName);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.httpContext = HttpClientContext.create();
        this.queue = queue;
        this.startupLatch = startupLatch;
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
            HttpTask task = queue.take();
            if (task.shouldShutdown() || running.get() == false) {
                running.set(false);
                logger.debug(() -> format("Http executor service [%s] exiting", serviceName));
            } else {
                executeTask(task);
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            logger.warn(format("Http executor service [%s] failed while retrieving task for execution", serviceName), e);
        }
    }

    private void executeTask(HttpTask task) {
        try {
            task.run();
        } catch (Exception e) {
            logger.warn(format("Http executor service [%s] failed to execute request [%s]", serviceName, task), e);
        }
    }

    private synchronized void notifyRequestsOfShutdown() {
        assert isShutdown() : "Requests should only be notified if the executor is shutting down";

        try {
            List<HttpTask> notExecuted = new ArrayList<>();
            queue.drainTo(notExecuted);

            for (HttpTask task : notExecuted) {
                rejectTask(task);
            }
        } catch (Exception e) {
            logger.warn(format("Failed to notify tasks of queuing service [%s] shutdown", serviceName));
        }
    }

    private void rejectTask(HttpTask task) {
        try {
            task.onRejection(
                new EsRejectedExecutionException(
                    format("Failed to send request, queue service [%s] has shutdown prior to executing request", serviceName),
                    true
                )
            );
        } catch (Exception e) {
            logger.warn(
                format("Failed to notify request [%s] for service [%s] of rejection after queuing service shutdown", task, serviceName)
            );
        }
    }

    public int queueSize() {
        return queue.size();
    }

    @Override
    public void shutdown() {
        if (running.compareAndSet(true, false)) {
            // if this fails because the queue is full, that's ok, we just want to ensure that queue.take() returns
            queue.offer(new ShutdownTask());
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        shutdown();
        return new ArrayList<>(queue);
    }

    @Override
    public boolean isShutdown() {
        return running.get() == false;
    }

    @Override
    public boolean isTerminated() {
        return terminationLatch.getCount() == 0;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    /**
     * Send the request at some point in the future.
     * @param request the http request to send
     * @param timeout the maximum time to wait for this request to complete (failing or succeeding). Once the time elapses, the
     *                listener::onFailure is called with a {@link org.elasticsearch.ElasticsearchTimeoutException}.
     *                If null, then the request will wait forever
     * @param listener an {@link ActionListener<HttpResult>} for the response or failure
     */
    public void send(HttpRequestBase request, @Nullable TimeValue timeout, ActionListener<HttpResult> listener) {
        RequestTask task = new RequestTask(request, httpClient, httpContext, timeout, threadPool, listener);

        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to enqueue task because the http executor service [%s] has already shutdown", serviceName),
                true
            );

            task.onRejection(rejected);
            return;
        }

        boolean added = queue.offer(task);
        if (added == false) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to execute task because the http executor service [%s] queue is full", serviceName),
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

    /**
     * This method is not supported. Use {@link #send} instead.
     * @param runnable the runnable task
     */
    @Override
    public void execute(Runnable runnable) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> Future<T> submit(Callable<T> task) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public Future<?> submit(Runnable task) {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        throw new UnsupportedOperationException("use send instead");
    }

    /**
     * This method is not supported. Use {@link #send} instead.
     */
    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
        throw new UnsupportedOperationException("use send instead");
    }
}
