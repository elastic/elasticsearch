/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.util.concurrent.EsExecutorService.ExecutionTimeTrackingEsExecutorService;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * An extension to thread pool executor, which tracks statistics for the task execution time.
 */
public final class TaskExecutionTimeTrackingEsThreadPoolExecutor extends EsThreadPoolExecutor
    implements
        ExecutionTimeTrackingEsExecutorService {
    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final TaskTracker taskTracker;

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
        TaskTrackingConfig trackingConfig
    ) {
        super(name, corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler, contextHolder);

        this.runnableWrapper = runnableWrapper;
        this.taskTracker = new TaskTracker(trackingConfig, maximumPoolSize);
    }

    public Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        return Stream.concat(super.setupMetrics(meterRegistry, threadPoolName), taskTracker.setupMetrics(meterRegistry, threadPoolName));
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
        return taskTracker.getTaskExecutionEWMA();
    }

    /**
     * Returns the total time (in nanoseconds) spend executing tasks in this executor.
     */
    public long getTotalTaskExecutionTime() {
        return taskTracker.getTotalTaskExecutionTime();
    }

    public long getMaxQueueLatencyMillisSinceLastPollAndReset() {
        return taskTracker.getMaxQueueLatencyMillisSinceLastPollAndReset();
    }

    /**
     * Returns the fraction of the maximum possible thread time that was actually used since the last time this method was called.
     * There are two periodic pulling mechanisms that access utilization reporting: {@link UtilizationTrackingPurpose} distinguishes the
     * caller.
     *
     * @return the utilization as a fraction, in the range [0, 1]. This may return >1 if a task completed in the time range but started
     * earlier, contributing a larger execution time.
     */
    public double pollUtilization(UtilizationTrackingPurpose utilizationTrackingPurpose) {
        return taskTracker.pollUtilization(utilizationTrackingPurpose);
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        taskTracker.trackTask(r);
        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        timedRunnable.beforeExecute();
        taskTracker.taskQueueLatency(timedRunnable.getQueueTimeNanos());
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
                taskTracker.taskExecutionTime(taskExecutionNanos);
            }
        } finally {
            taskTracker.untrackTask(r);
        }
    }

    @Override
    protected void appendThreadPoolExecutorDetails(StringBuilder sb) {
        taskTracker.appendTaskExecutionDetails(sb);
    }

    /**
     * Returns the set of currently running tasks and their start timestamp.
     * <p>
     * Note that it is possible for a task that has just finished execution to be temporarily both in the returned map, and its total
     * execution time to be included in the return value of {@code getTotalTaskExecutionTime()}. However, it is guaranteed that the
     * task is reflected in at least one of those two values.
     */
    public Map<Runnable, Long> getOngoingTasks() {
        return taskTracker.getOngoingTasks();
    }

    // Used for testing
    public double getExecutionEwmaAlpha() {
        return taskTracker.getExecutionEwmaAlpha();
    }

    // Used for testing
    public boolean trackingMaxQueueLatency() {
        return taskTracker.trackingMaxQueueLatency();
    }
}
