/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.core.SuppressForbidden;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * An extension to thread pool executor, allowing (in the future) to add specific additional stats to it.
 */
public class EsThreadPoolExecutor extends ThreadPoolExecutor {

    private final ThreadContext contextHolder;
    private volatile ShutdownListener listener;

    private final Object monitor = new Object();
    /**
     * Name used in error reporting.
     */
    private final String name;

    final String getName() {
        return name;
    }

    EsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, ThreadContext contextHolder) {
        this(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, new EsAbortPolicy(), contextHolder);
    }

    @SuppressForbidden(reason = "properly rethrowing errors, see EsExecutors.rethrowErrors")
    EsThreadPoolExecutor(String name, int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
            BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, XRejectedExecutionHandler handler,
            ThreadContext contextHolder) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
        this.name = name;
        this.contextHolder = contextHolder;
    }

    @Override
    protected synchronized void terminated() {
        super.terminated();
        synchronized (monitor) {
            if (listener != null) {
                try {
                    listener.onTerminated();
                } finally {
                    listener = null;
                }
            }
        }
    }

    public interface ShutdownListener {
        void onTerminated();
    }

    @Override
    public void execute(Runnable command) {
        command = wrapRunnable(command);
        try {
            super.execute(command);
        } catch (EsRejectedExecutionException ex) {
            if (command instanceof AbstractRunnable) {
                // If we are an abstract runnable we can handle the rejection
                // directly and don't need to rethrow it.
                try {
                    ((AbstractRunnable) command).onRejection(ex);
                } finally {
                    ((AbstractRunnable) command).onAfter();

                }
            } else {
                throw ex;
            }
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        EsExecutors.rethrowErrors(unwrap(r));
        assert assertDefaultContext(r);
    }

    private boolean assertDefaultContext(Runnable r) {
        assert contextHolder.isDefaultContext() : "the thread context is not the default context and the thread [" +
            Thread.currentThread().getName() + "] is being returned to the pool after executing [" + r + "]";
        return true;
    }

    /**
     * Returns a stream of all pending tasks. This is similar to {@link #getQueue()} but will expose the originally submitted
     * {@link Runnable} instances rather than potentially wrapped ones.
     */
    public Stream<Runnable> getTasks() {
        return this.getQueue().stream().map(this::unwrap);
    }

    @Override
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('[');
        b.append("name = ").append(name).append(", ");
        if (getQueue() instanceof SizeBlockingQueue) {
            @SuppressWarnings("rawtypes")
            SizeBlockingQueue queue = (SizeBlockingQueue) getQueue();
            b.append("queue capacity = ").append(queue.capacity()).append(", ");
        }
        appendThreadPoolExecutorDetails(b);
        /*
         * ThreadPoolExecutor has some nice information in its toString but we
         * can't get at it easily without just getting the toString.
         */
        b.append(super.toString()).append(']');
        return b.toString();
    }

    /**
     * Append details about this thread pool to the specified {@link StringBuilder}. All details should be appended as key/value pairs in
     * the form "%s = %s, "
     *
     * @param sb the {@link StringBuilder} to append to
     */
    protected void appendThreadPoolExecutorDetails(final StringBuilder sb) {

    }

    protected Runnable wrapRunnable(Runnable command) {
        return contextHolder.preserveContext(command);
    }

    protected Runnable unwrap(Runnable runnable) {
        return contextHolder.unwrap(runnable);
    }
}
