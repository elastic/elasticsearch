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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.external.http.HttpClient;
import org.elasticsearch.xpack.inference.external.http.HttpResult;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
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
class HttpRequestExecutorService extends AbstractExecutorService {
    private static final Logger logger = LogManager.getLogger(HttpRequestExecutorService.class);

    private final ThreadContext contextHolder;
    private final String serviceName;
    private final BlockingQueue<HttpTask> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final HttpClientContext httpContext;
    private final HttpClient httpClient;
    private final ThreadPool threadPool;

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    HttpRequestExecutorService(ThreadContext contextHolder, String serviceName, HttpClient httpClient, ThreadPool threadPool) {
        this(contextHolder, serviceName, httpClient, threadPool, null);
    }

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    HttpRequestExecutorService(
        ThreadContext contextHolder,
        String serviceName,
        HttpClient httpClient,
        ThreadPool threadPool,
        @Nullable Integer capacity
    ) {
        this.contextHolder = Objects.requireNonNull(contextHolder);
        this.serviceName = Objects.requireNonNull(serviceName);
        this.httpClient = Objects.requireNonNull(httpClient);
        this.threadPool = Objects.requireNonNull(threadPool);
        this.httpContext = HttpClientContext.create();

        if (capacity == null) {
            this.queue = new LinkedBlockingQueue<>();
        } else {
            this.queue = new LinkedBlockingQueue<>(capacity);
        }
    }

    /**
     * Begin servicing tasks.
     */
    public void start() {
        try {
            while (running.get()) {
                HttpTask task = queue.take();
                if (task.shouldShutdown() || running.get() == false) {
                    running.set(false);
                    logger.debug(() -> format("Http executor service [%s] exiting", serviceName));
                } else {
                    executeTask(task);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            notifyRequestsOfShutdown();
            terminationLatch.countDown();
        }
    }

    private void executeTask(HttpTask task) {
        try {
            contextHolder.preserveContext(task).run();
        } catch (Exception e) {
            logger.warn(format("Http executor service [%s] failed to execute request [%s]", serviceName, task), e);
        }
        EsExecutors.rethrowErrors(ThreadContext.unwrap(task));
    }

    private void notifyRequestsOfShutdown() {
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
                new EsRejectedExecutionException(format("Failed to send request, queue service [%s] has shutdown", serviceName), true)
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
     * @param timeout the maximum time to wait for this request to complete (failing or successfully). Once the time elapses, the
     *                listener::onFailure is called with a {@link java.util.concurrent.TimeoutException}. If null, then the request will
     *                wait forever
     * @param listener an {@link ActionListener<HttpResult>} for the response or failure
     */
    public void send(HttpRequestBase request, @Nullable TimeValue timeout, ActionListener<HttpResult> listener) {
        RequestTask task = new RequestTask(request, httpClient, httpContext, timeout, threadPool, listener);

        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Failed to execute task because the http executor service [%s] has shutdown", serviceName),
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
}
