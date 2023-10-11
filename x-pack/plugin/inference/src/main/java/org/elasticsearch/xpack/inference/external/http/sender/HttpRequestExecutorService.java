/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.http.client.protocol.HttpClientContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;

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
public class HttpRequestExecutorService extends AbstractExecutorService {
    private static final Logger logger = LogManager.getLogger(HttpRequestExecutorService.class);

    private final ThreadContext contextHolder;
    private final String serviceName;
    private final BlockingQueue<HttpTask> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final HttpClientContext httpContext;

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public HttpRequestExecutorService(ThreadContext contextHolder, String serviceName) {
        this(contextHolder, serviceName, null);
    }

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public HttpRequestExecutorService(ThreadContext contextHolder, String serviceName, @Nullable Integer capacity) {
        this.contextHolder = Objects.requireNonNull(contextHolder);
        this.serviceName = Objects.requireNonNull(serviceName);
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
            terminationLatch.countDown();
        }
    }

    private void executeTask(HttpTask task) {
        try {
            contextHolder.preserveContext(task).run();
        } catch (Exception e) {
            logger.error(format("Http executor service [%s] failed to execute request [%s]", serviceName, task), e);
        }
        EsExecutors.rethrowErrors(ThreadContext.unwrap(task));
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
     * Execute the task at some point in the future.
     * @param command the runnable task, must be a class that extends {@link HttpTask}
     */
    @Override
    public void execute(Runnable command) {
        if (command == null) {
            return;
        }

        assert command instanceof HttpTask;
        HttpTask task = (HttpTask) command;
        task.setContext(httpContext);

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
}
