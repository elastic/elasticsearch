/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor.WORKER_PROBE;
import static org.elasticsearch.core.Strings.format;

/**
 * There is a subtle interaction between a scaling {@link org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor} with
 * {@link org.elasticsearch.threadpool.ScalingExecutorBuilder.ScalingExecutorSettings#rejectAfterShutdown} set to false, and
 * the {@link org.elasticsearch.common.util.concurrent.ThrottledTaskRunner}.
 * <p>
 * When a {@link org.elasticsearch.common.util.concurrent.ThrottledTaskRunner} is feeding into a scaling executor that doesn't
 * reject after shutdown, it will always be fully processed in the event of a shutdown. This is because whenever a throttled
 * task finishes, it checks if there are more queued and forces them onto the end of the thread-pool queue even though they
 * are rejected by {@link java.util.concurrent.ThreadPoolExecutor#execute(Runnable)}. The executor won't terminate until all
 * workers are finished and the queue is empty, so the fact each worker adds a task to the end of the queue before it terminates
 * means the {@link org.elasticsearch.common.util.concurrent.ThrottledTaskRunner} queue will be drained before the thread pool
 * executor terminates.
 * <p>
 * This decorator attempts to emulate that behavior in the absence of an explicit queue, and also ensures that {@link ThreadContext}
 * is propagated to tasks that are dispatched.
 */
public class EsExecutorServiceDecorator implements ExecutorService {

    private static final Logger logger = LogManager.getLogger(EsExecutorServiceDecorator.class);

    private final String name;
    private final ExecutorService delegate;
    private final ThreadContext contextHolder;
    private final boolean rejectAfterShutdown;
    private final AtomicInteger runningTasks = new AtomicInteger();
    private final AtomicBoolean shutdownRequested = new AtomicBoolean(false);

    public EsExecutorServiceDecorator(String name, ExecutorService delegate, ThreadContext contextHolder, boolean rejectAfterShutdown) {
        this.name = name;
        this.delegate = delegate;
        this.contextHolder = contextHolder;
        this.rejectAfterShutdown = rejectAfterShutdown;
    }

    @Override
    public void shutdown() {
        shutdownRequested.set(true);
        tryShutdownDelegate();
    }

    private void tryShutdownDelegate() {
        if (shutdownRequested.get() && runningTasks.compareAndSet(0, -1)) {
            delegate.shutdown();
        }
    }

    @Override
    public List<Runnable> shutdownNow() {
        return delegate.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return delegate.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return delegate.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long endTime = System.nanoTime() + unit.toNanos(timeout);
        shutdownRequested.set(true);
        while (delegate.isShutdown() == false) {
            if (System.nanoTime() > endTime) {
                return false;
            }
            Thread.sleep(1);
            tryShutdownDelegate();
        }
        logger.info("Falling through");
        return delegate.awaitTermination(timeout, unit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return delegate.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return delegate.submit(task, result);
    }

    @Override
    public Future<?> submit(Runnable task) {
        return delegate.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return delegate.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return delegate.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return delegate.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException,
        ExecutionException, TimeoutException {
        return delegate.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        final Runnable wrappedRunnable = command != WORKER_PROBE ? wrapRunnable(command) : WORKER_PROBE;
        try {
            if (rejectAfterShutdown && (shutdownRequested.get() || delegate.isShutdown())) {
                throw new EsRejectedExecutionException("executor has been shutdown", delegate.isShutdown());
            }
            // Increment outstanding task count
            runningTasks.getAndUpdate(currentValue -> {
                if (currentValue == -1) {
                    throw new EsRejectedExecutionException("executor has been shutdown", true);
                } else {
                    return currentValue + 1;
                }
            });
            try {
                delegate.execute(() -> {
                    try {
                        wrappedRunnable.run();
                    } finally {
                        // Decrement outstanding
                        runningTasks.decrementAndGet();
                        tryShutdownDelegate();
                    }
                });
            } catch (RejectedExecutionException e) {
                if (command == WORKER_PROBE) {
                    return;
                }
                throw new EsRejectedExecutionException("delegate rejected execution", delegate.isShutdown());
            }
        } catch (Exception e) {
            if (wrappedRunnable instanceof AbstractRunnable abstractRunnable) {
                try {
                    // If we are an abstract runnable we can handle the exception
                    // directly and don't need to rethrow it, but we log and assert
                    // any unexpected exception first.
                    if (e instanceof EsRejectedExecutionException == false) {
                        logException(abstractRunnable, e);
                    }
                    abstractRunnable.onRejection(e);
                } finally {
                    abstractRunnable.onAfter();
                }
            } else {
                throw e;
            }
        }
    }

    // package-visible for testing
    void logException(AbstractRunnable r, Exception e) {
        logger.error(() -> format("[%s] unexpected exception when submitting task [%s] for execution", name, r), e);
        assert false : "executor throws an exception (not a rejected execution exception) before the task has been submitted " + e;
    }

    protected Runnable wrapRunnable(Runnable command) {
        return contextHolder.preserveContext(command);
    }
}
