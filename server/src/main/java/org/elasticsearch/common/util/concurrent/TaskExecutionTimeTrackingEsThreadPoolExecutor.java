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
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.Instrument;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE_TIME;
import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION;

/**
 * An extension to thread pool executor, which tracks statistics for the task execution time.
 */
public final class TaskExecutionTimeTrackingEsThreadPoolExecutor extends EsThreadPoolExecutor {
    public static final int QUEUE_LATENCY_HISTOGRAM_BUCKETS = 18;
    private static final int[] LATENCY_PERCENTILES_TO_REPORT = { 50, 90, 99 };
    private static final long UTILISATION_REFRESH_INTERVAL_NANOS = TimeValue.timeValueSeconds(45).nanos();

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final LongAdder totalExecutionTime = new LongAdder();
    private final boolean trackOngoingTasks;
    // The set of currently running tasks and the timestamp of when they started execution in the Executor.
    private final Map<Runnable, Long> ongoingTasks = new ConcurrentHashMap<>();
    private final ExponentialBucketHistogram queueLatencyMillisHistogram = new ExponentialBucketHistogram(QUEUE_LATENCY_HISTOGRAM_BUCKETS);
    private final boolean trackMaxQueueLatency;
    private final LongAccumulator maxQueueLatencyMillisSinceLastPoll = new LongAccumulator(Long::max, 0);
    private final UtilizationTracker utilizationTracker = new UtilizationTracker();

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
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(trackingConfig.getExecutionTimeEwmaAlpha(), 0);
        this.trackOngoingTasks = trackingConfig.trackOngoingTasks();
        this.trackMaxQueueLatency = trackingConfig.trackMaxQueueLatency();
    }

    public List<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        return List.of(
            meterRegistry.registerLongsGauge(
                ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName + THREAD_POOL_METRIC_NAME_QUEUE_TIME,
                "Time tasks spent in the queue for the " + threadPoolName + " thread pool",
                "milliseconds",
                () -> {
                    long[] snapshot = queueLatencyMillisHistogram.getSnapshot();
                    int[] bucketUpperBounds = queueLatencyMillisHistogram.calculateBucketUpperBounds();
                    List<LongWithAttributes> metricValues = Arrays.stream(LATENCY_PERCENTILES_TO_REPORT)
                        .mapToObj(
                            percentile -> new LongWithAttributes(
                                queueLatencyMillisHistogram.getPercentile(percentile / 100f, snapshot, bucketUpperBounds),
                                Map.of("percentile", String.valueOf(percentile))
                            )
                        )
                        .toList();
                    queueLatencyMillisHistogram.clear();
                    return metricValues;
                }
            ),
            meterRegistry.registerDoubleGauge(
                ThreadPool.THREAD_POOL_METRIC_PREFIX + threadPoolName + THREAD_POOL_METRIC_NAME_UTILIZATION,
                "fraction of maximum thread time utilized for " + threadPoolName,
                "fraction",
                () -> new DoubleWithAttributes(getUtilization(), Map.of())
            )
        );
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

    public long getMaxQueueLatencyMillisSinceLastPollAndReset() {
        if (trackMaxQueueLatency == false) {
            return 0;
        }
        return maxQueueLatencyMillisSinceLastPoll.getThenReset();
    }

    /**
     * Returns the fraction of the maximum possible thread time that was actually used recently.
     *
     * This value is updated approximately every {@link #UTILISATION_REFRESH_INTERVAL_NANOS}
     *
     * @return the utilization as a fraction, in the range [0, 1]. This may return >1 if a task completed in the time range but started
     * earlier, contributing a larger execution time.
     */
    public double getUtilization() {
        return utilizationTracker.getUtilization();
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (trackOngoingTasks) {
            ongoingTasks.put(r, System.nanoTime());
        }

        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        timedRunnable.beforeExecute();
        final long taskQueueLatency = timedRunnable.getQueueTimeNanos();
        assert taskQueueLatency >= 0;
        var queueLatencyMillis = TimeUnit.NANOSECONDS.toMillis(taskQueueLatency);
        queueLatencyMillisHistogram.addObservation(queueLatencyMillis);

        if (trackMaxQueueLatency) {
            maxQueueLatencyMillisSinceLastPoll.accumulate(queueLatencyMillis);
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
            utilizationTracker.recalculateUtilizationIfDue();
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
    public double getExecutionEwmaAlpha() {
        return executionEWMA.getAlpha();
    }

    // Used for testing
    public boolean trackingMaxQueueLatency() {
        return trackMaxQueueLatency;
    }

    /**
     * Tracks the utilization of a thread pool by periodically calculating the average since the last time it was calculated. Requires
     * that {@link #recalculateUtilizationIfDue()} is called regularly to stay up to date.
     *
     * Uses the difference of {@link #totalExecutionTime} since the last calculation to determine how much activity has occurred.
     */
    private class UtilizationTracker {
        final AtomicLong lastCalculatedTime = new AtomicLong(System.nanoTime());
        volatile long lastTotalExecutionTime = 0;
        volatile double lastUtilization = 0;

        /**
         * If our utilization value is stale, recalculate it
         */
        public void recalculateUtilizationIfDue() {
            final long now = System.nanoTime();
            final long lastCalcTimeCopy = lastCalculatedTime.get();
            if (now - lastCalcTimeCopy > UTILISATION_REFRESH_INTERVAL_NANOS) {

                // UTILISATION_REFRESH_INTERVAL should be large enough that this
                // compare-and-swap is enough to avoid concurrency issues here
                if (lastCalculatedTime.compareAndSet(lastCalcTimeCopy, now)) {
                    final long currentTotalExecutionTimeNanos = totalExecutionTime.sum();

                    final long totalExecutionTimeSinceLastPollNanos = currentTotalExecutionTimeNanos - lastTotalExecutionTime;
                    final long timeSinceLastPoll = now - lastCalcTimeCopy;

                    final long maximumExecutionTimeSinceLastPollNanos = timeSinceLastPoll * getMaximumPoolSize();
                    final double utilizationSinceLastPoll = (double) totalExecutionTimeSinceLastPollNanos
                        / maximumExecutionTimeSinceLastPollNanos;

                    lastTotalExecutionTime = currentTotalExecutionTimeNanos;
                    lastUtilization = utilizationSinceLastPoll;
                }
            }
        }

        /**
         * Get the most recent utilization value calculated
         */
        public double getUtilization() {
            return lastUtilization;
        }
    }
}
