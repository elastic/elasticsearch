/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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

public class HttpRequestExecutorService extends AbstractExecutorService {
    private static final Logger logger = LogManager.getLogger(HttpRequestExecutorService.class);

    private final ThreadContext contextHolder;
    private final String serviceName;
    private final BlockingQueue<HttpTask> queue;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final CountDownLatch terminationLatch = new CountDownLatch(1);

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    public HttpRequestExecutorService(ThreadContext contextHolder, String serviceName) {
        this.contextHolder = Objects.requireNonNull(contextHolder);
        this.serviceName = Objects.requireNonNull(serviceName);
        this.queue = new LinkedBlockingQueue<>();
    }

    public void start() {
        try {
            while (running.get()) {
                HttpTask task = queue.take();
                logger.error("got a task");
                if (task.shouldShutdown() || running.get() == false) {
                    logger.error("shutting down " + task.shouldShutdown() + " " + running.get());
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
        running.set(false);
        execute(new ShutdownTask());
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

    @Override
    public void execute(Runnable command) {
        if (command == null) {
            return;
        }

        if (isShutdown()) {
            EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                format("Http executor service [%s] has shutdown", serviceName),
                true
            );

            rejectCommand(command, rejected);
            return;
        }

        assert command instanceof HttpTask;
        HttpTask task = (HttpTask) command;

        boolean added = queue.offer(task);
        if (added == false) {
            rejectCommand(
                task,
                new EsRejectedExecutionException(
                    format("Http executor service [%s] queue is full. Unable to execute request", serviceName),
                    false
                )
            );
        }
    }

    private void rejectCommand(Runnable command, EsRejectedExecutionException exception) {
        if (command instanceof AbstractRunnable runnable) {
            runnable.onRejection(exception);
        } else {
            throw exception;
        }
    }
}
