/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import static org.elasticsearch.core.Strings.format;

// special cases:
// - EsAbortPolicy: isForceExecution allows to bypass queue limit
// - ForceQueuePolicy: rejectAfterShutdown
// FIXME thread names must include nodeName
public class EsVirtualThreadExecutorService extends AbstractExecutorService implements EsExecutorService {
    private static final Logger logger = LogManager.getLogger(EsVirtualThreadExecutorService.class);
    private static final VarHandle PENDING_HANDLE;

    static {
        try {
            PENDING_HANDLE = MethodHandles.lookup().findVarHandle(EsVirtualThreadExecutorService.class, "pendingTasks", int.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    private final String name;
    private final ExecutorService virtualExecutor;
    private final Semaphore concurrencyLimit;
    private final int maximumPoolSize;
    private final int maximumQueueSize;
    private final ThreadContext contextHolder;

    private final LongAdder completed = new LongAdder();
    private volatile int largestPoolSize = 0;
    private volatile int pendingTasks = 0;

    public EsVirtualThreadExecutorService(String name, int maximumPoolSize, int maximumQueueSize, ThreadContext contextHolder) {
        this.name = name;
        this.virtualExecutor = Executors.newThreadPerTaskExecutor(virtualThreadFactory(name));
        this.concurrencyLimit = new Semaphore(maximumPoolSize, true);
        this.maximumPoolSize = maximumPoolSize;
        this.maximumQueueSize = maximumQueueSize;
        this.contextHolder = contextHolder;
    }

    // FIXME remove hack, should be pushed up
    private static ThreadFactory virtualThreadFactory(String name) {
        String[] split = name.split("/", 2);
        name = split.length == 2 ? EsExecutors.threadName(split[0], split[1]) : EsExecutors.threadName("", split[0]);
        return Thread.ofVirtual().name(name, 0).factory();
    }

    @Override
    public int getActiveCount() {
        return maximumPoolSize - concurrencyLimit.availablePermits();
    }

    @Override
    public long getCompletedTaskCount() {
        return completed.sum();
    }

    @Override
    public int getCurrentQueueSize() {
        return Math.max(0, pendingTasks - maximumPoolSize);
    }

    @Override
    public int getMaximumPoolSize() {
        return maximumPoolSize;
    }

    @Override
    public int getPoolSize() {
        return getActiveCount();
    }

    @Override
    public int getLargestPoolSize() {
        return largestPoolSize;
    }

    private class ThrottledRunnable implements Runnable {
        private final Runnable r;

        ThrottledRunnable(Runnable r) {
            this.r = r;
        }

        @Override
        public void run() {
            try {
                long creationTimeNanos = System.nanoTime();
                concurrencyLimit.acquire();
                largestPoolSize = Math.max(getActiveCount(), largestPoolSize); // decent estimate
                try {
                    doExecute(r, creationTimeNanos);
                } finally {
                    concurrencyLimit.release();
                }
            } catch (InterruptedException e) {
                // FIXME how to handle interrupts?
            } finally {
                decrementPendingTasks();
                completed.increment();
            }
        }
    }

    // beforeExecute / afterExecute
    protected void doExecute(Runnable r, long creationTimeNanos) {
        r.run();
        EsExecutors.rethrowErrors(unwrap(r));
        assert assertDefaultContext(r);
    }

    // FIXME is this needed with virtual threads?
    private boolean assertDefaultContext(Runnable r) {
        assert contextHolder.isDefaultContext()
            : "the thread context is not the default context and the thread ["
                + Thread.currentThread().getName()
                + "] is being returned to the pool after executing ["
                + r
                + "]";
        return true;
    }

    @Override
    public void execute(Runnable command) {
        // FIXME needs to support `void afterExecute(Runnable r, Throwable t)`
        final Runnable wrappedRunnable = wrapRunnable(command);
        try {
            if (incrementPendingTasks()) {
                try {
                    // FIXME handle rejections & associated metrics (see EsRejectedExecutionHandler)
                    virtualExecutor.execute(new ThrottledRunnable(wrappedRunnable));
                } catch (Exception e) {
                    decrementPendingTasks();
                    throw e;
                }
            } else {
                throw EsRejectedExecutionHandler.newRejectedException(wrappedRunnable, this, virtualExecutor.isShutdown());
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

    private boolean incrementPendingTasks() {
        while (true) {
            int pending = pendingTasks;
            if (maximumQueueSize >= 0 && pending >= maximumQueueSize + maximumPoolSize) {
                return false;
            }
            if (PENDING_HANDLE.weakCompareAndSet(this, pending, pending + 1)) {
                return true;
            }
        }
    }

    private void decrementPendingTasks() {
        while (true) {
            int pending = pendingTasks;
            if (PENDING_HANDLE.weakCompareAndSet(this, pending, pending - 1)) {
                return;
            }
        }
    }

    // package-visible for testing
    void logException(AbstractRunnable r, Exception e) {
        logger.error(() -> format("[%s] unexpected exception when submitting task [%s] for execution", name, r), e);
        assert false : "executor throws an exception (not a rejected execution exception) before the task has been submitted " + e;
    }

    @Override
    public final String toString() {
        StringBuilder b = new StringBuilder();
        b.append(getClass().getSimpleName()).append('[');
        b.append("name = ").append(name).append(", ");
        if (maximumQueueSize >= 0) {
            b.append("queue capacity = ").append(maximumQueueSize).append(", ");
        }
        appendExecutorDetails(b);
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
    protected void appendExecutorDetails(final StringBuilder sb) {}

    public void shutdown() {
        virtualExecutor.shutdown();
    }

    public List<Runnable> shutdownNow() {
        return virtualExecutor.shutdownNow();
    }

    public boolean isShutdown() {
        return virtualExecutor.isShutdown();
    }

    public boolean isTerminated() {
        return virtualExecutor.isTerminated();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return virtualExecutor.awaitTermination(timeout, unit);
    }

    protected Runnable wrapRunnable(Runnable command) {
        return contextHolder.preserveContext(command);
    }

    protected Runnable unwrap(Runnable runnable) {
        return ThreadContext.unwrap(runnable);
    }
}
