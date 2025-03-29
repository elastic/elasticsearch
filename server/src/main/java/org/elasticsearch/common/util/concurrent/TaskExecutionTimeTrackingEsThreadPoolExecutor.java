/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.common.ExponentiallyWeightedMovingAverage;
import org.elasticsearch.common.metrics.ExponentialBucketHistogram;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
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

    private static final int[] LATENCY_PERCENTILES_TO_REPORT = { 50, 90 };
    public static final String THREAD_POOL_METRIC_NAME_QUEUE_TIME = ".threads.queue.latency.histogram";

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final LongAdder totalExecutionTime = new LongAdder();
    private final boolean trackOngoingTasks;
    // The set of currently running tasks and the timestamp of when they started execution in the Executor.
    private final Map<Runnable, Long> ongoingTasks = new ConcurrentHashMap<>();
    private final ExponentialBucketHistogram queueLatencyHistogram = new ExponentialBucketHistogram();

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
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(trackingConfig.getEwmaAlpha(), 0);
        this.trackOngoingTasks = trackingConfig.trackOngoingTasks();
    }

    public List<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        final LongGauge queueLatencyGauge = meterRegistry.registerLongsGauge(
            ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName + THREAD_POOL_METRIC_NAME_QUEUE_TIME,
            "Time tasks spent in the queue for the " + threadPoolName + " thread pool",
            "milliseconds",
            () -> {
                List<LongWithAttributes> metricValues = Arrays.stream(LATENCY_PERCENTILES_TO_REPORT)
                    .mapToObj(
                        percentile -> new LongWithAttributes(
                            queueLatencyHistogram.getPercentile(percentile / 100f),
                            Map.of("percentile", String.valueOf(percentile))
                        )
                    )
                    .toList();
                queueLatencyHistogram.clear();
                return metricValues;
            }
        );
        return List.of(queueLatencyGauge);
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
        if (trackOngoingTasks) {
            ongoingTasks.put(r, System.nanoTime());
        }
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
            final long taskQueueLatency = timedRunnable.getQueueTimeNanos();
            assert taskQueueLatency >= 0 || (failedOrRejected && taskQueueLatency == -1)
                : "queue latency should always be non-negative or `-1` to indicate rejection, got: "
                    + taskQueueLatency
                    + ", failedOrRejected: "
                    + failedOrRejected;
            if (taskQueueLatency != -1) {
                queueLatencyHistogram.addObservation(TimeUnit.NANOSECONDS.toMillis(taskQueueLatency));
            }
        } finally {
            // if trackOngoingTasks is false -> ongoingTasks must be empty
            assert trackOngoingTasks || ongoingTasks.isEmpty();
            if (trackOngoingTasks) {
                ongoingTasks.remove(r);
            }
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
        return trackOngoingTasks ? Map.copyOf(ongoingTasks) : Map.of();
    }

    // Used for testing
    public double getEwmaAlpha() {
        return executionEWMA.getAlpha();
    }
}
