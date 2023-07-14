/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.core.TimeValue;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

/**
 * An extension to thread pool executor, which tracks statistics for the task execution time.
 */
public final class TaskExecutionTimeTrackingEsThreadPoolExecutor extends EsThreadPoolExecutor {

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final LongAdder totalExecutionTime = new LongAdder();
    private final Map<Runnable, Long> ongoingTasks = new ConcurrentHashMap<>();

    TaskExecutionTimeTrackingEsThreadPoolExecutor(
        String name,
        int corePoolSize,
        int maximumPoolSize,
        long keepAliveTime,
        TimeUnit unit,
        BlockingQueue<Runnable> workQueue,
        Function<Runnable, WrappedRunnable> runnableWrapper,
        ThreadFactory threadFactory,
        RejectedExecutionHandler handler,
        ThreadContext contextHolder,
        double ewmaAlpha
    ) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);
        this.runnableWrapper = runnableWrapper;
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(ewmaAlpha, 0);
    }

    @Override
    protected Runnable wrapRunnable(Runnable command) {
        return super.wrapRunnable(this.runnableWrapper.apply(command));
    }

    @Override
    protected Runnable unwrap(Runnable runnable) {
        final Runnable unwrapped = super.unwrap(runnable);
        if (unwrapped instanceof WrappedRunnable) {
            return ((WrappedRunnable) unwrapped).unwrap();
        } else {
            return unwrapped;
        }
    }

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    public double getTaskExecutionEWMA() {
        return executionEWMA.getAverage();
    }

    /**
     * Returns the total time (in nanoseconds) spend executing tasks in this executor.
     */
    public long getTotalTaskExecutionTime() {
        return totalExecutionTime.sum();
    }

    /**
     * Returns the current queue size (operations that are queued)
     */
    public int getCurrentQueueSize() {
        return getQueue().size();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        super.beforeExecute(t, r);
        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        ongoingTasks.put(r, timedRunnable.getStartTimeNanos());
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        try {
            super.afterExecute(r, t);
            // A task has been completed, it has left the building. We should now be able to get the
            // total time as a combination of the time in the queue and time spent running the task. We
            // only want runnables that did not throw errors though, because they could be fast-failures
            // that throw off our timings, so only check when t is null.
            assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
            final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
            final boolean failedOrRejected = timedRunnable.getFailedOrRejected();
            final long taskExecutionNanos = timedRunnable.getTotalExecutionNanos();
            assert taskExecutionNanos >= 0 || (failedOrRejected && taskExecutionNanos == -1)
                : "expected task to always take longer than 0 nanoseconds or have '-1' failure code, got: "
                    + taskExecutionNanos
                    + ", failedOrRejected: "
                    + failedOrRejected;
            if (taskExecutionNanos != -1) {
                // taskExecutionNanos may be -1 if the task threw an exception
                executionEWMA.addValue(taskExecutionNanos);
                totalExecutionTime.add(taskExecutionNanos);
            }
        } finally {
            ongoingTasks.remove(r);
        }
    }

    @Override
    protected void appendThreadPoolExecutorDetails(StringBuilder sb) {
        sb.append("task execution EWMA = ")
            .append(TimeValue.timeValueNanos((long) executionEWMA.getAverage()))
            .append(", ")
            .append("total task execution time = ")
            .append(TimeValue.timeValueNanos(getTotalTaskExecutionTime()))
            .append(", ");
    }

    /**
     * Returns the set of currently running tasks and their start timestamp.
     * <p>
     * Note that it is possible for a task that has just finished execution to be temporarily both in the returned map, and its total
     * execution time to be included in the return value of {@code getTotalTaskExecutionTime()}. However, it is guaranteed that the
     * task is reflected in at least one of those two values.
     */
    public Map<Runnable, Long> getOngoingTasks() {
        return Map.copyOf(ongoingTasks);
    }
}
