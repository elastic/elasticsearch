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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Stream;

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE_TIME;
import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION;

class TaskTracker {
    static final int QUEUE_LATENCY_HISTOGRAM_BUCKETS = 18;
    private static final int[] LATENCY_PERCENTILES_TO_REPORT = { 50, 90, 99 };

    private final int maximumPoolSize;

    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final LongAdder totalExecutionTime = new LongAdder();
    private final boolean trackOngoingTasks;
    // The set of currently running tasks and the timestamp of when they started execution in the Executor.
    private final Map<Runnable, Long> ongoingTasks;
    private final ExponentialBucketHistogram queueLatencyMillisHistogram = new ExponentialBucketHistogram(QUEUE_LATENCY_HISTOGRAM_BUCKETS);
    private final boolean trackMaxQueueLatency;
    private LongAccumulator maxQueueLatencyMillisSinceLastPoll = new LongAccumulator(Long::max, 0);

    private final UtilizationTracker apmUtilizationTracker = new UtilizationTracker();
    private final UtilizationTracker allocationUtilizationTracker = new UtilizationTracker();

    TaskTracker(EsExecutors.TaskTrackingConfig trackingConfig, int maximumPoolSize) {
        this.maximumPoolSize = maximumPoolSize;
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(trackingConfig.executionTimeEwmaAlpha(), 0);
        this.trackMaxQueueLatency = trackingConfig.trackMaxQueueLatency();
        this.trackOngoingTasks = trackingConfig.trackOngoingTasks();
        this.ongoingTasks = trackOngoingTasks ? new ConcurrentHashMap<>() : Collections.emptyMap();
    }

    Stream<Instrument> setupMetrics(MeterRegistry meterRegistry, String threadPoolName) {
        return Stream.of(
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
                () -> new DoubleWithAttributes(
                    pollUtilization(EsExecutorService.TaskTrackingEsExecutorService.UtilizationTrackingPurpose.APM),
                    Map.of()
                )
            )
        );
    }

    /**
     * Returns the exponentially weighted moving average of the task execution time
     */
    double getTaskExecutionEWMA() {
        return executionEWMA.getAverage();
    }

    /**
     * Returns the total time (in nanoseconds) spend executing tasks in this executor.
     */
    long getTotalTaskExecutionTime() {
        return totalExecutionTime.sum();
    }

    long getMaxQueueLatencyMillisSinceLastPollAndReset() {
        if (trackMaxQueueLatency == false) {
            return 0;
        }
        return maxQueueLatencyMillisSinceLastPoll.getThenReset();
    }

    /**
     * Returns the fraction of the maximum possible thread time that was actually used since the last time this method was called.
     * There are two periodic pulling mechanisms that access utilization reporting: {@link TaskTimeTrackingEsThreadPoolExecutor.UtilizationTrackingPurpose} distinguishes the
     * caller.
     *
     * @return the utilization as a fraction, in the range [0, 1]. This may return >1 if a task completed in the time range but started
     * earlier, contributing a larger execution time.
     */
    double pollUtilization(TaskTimeTrackingEsThreadPoolExecutor.UtilizationTrackingPurpose utilizationTrackingPurpose) {
        switch (utilizationTrackingPurpose) {
            case APM:
                return apmUtilizationTracker.pollUtilization();
            case ALLOCATION:
                return allocationUtilizationTracker.pollUtilization();
            default:
                throw new IllegalStateException("No operation defined for [" + utilizationTrackingPurpose + "]");
        }
    }

    /**
     * Returns the set of currently running tasks and their start timestamp.
     * <p>
     * Note that it is possible for a task that has just finished execution to be temporarily both in the returned map, and its total
     * execution time to be included in the return value of {@code getTotalTaskExecutionTime()}. However, it is guaranteed that the
     * task is reflected in at least one of those two values.
     */
    Map<Runnable, Long> getOngoingTasks() {
        return trackOngoingTasks ? Map.copyOf(ongoingTasks) : Collections.emptyMap();
    }

    void trackTask(Runnable r) {
        if (trackOngoingTasks) {
            ongoingTasks.put(r, System.nanoTime());
        }
    }

    void untrackTask(Runnable r) {
        // if trackOngoingTasks is false -> ongoingTasks must be empty
        assert trackOngoingTasks || ongoingTasks.isEmpty();
        if (trackOngoingTasks) {
            ongoingTasks.remove(r);
        }
    }

    void taskQueueLatency(long taskQueueTimeNanos) {
        assert taskQueueTimeNanos >= 0;
        var queueLatencyMillis = TimeUnit.NANOSECONDS.toMillis(taskQueueTimeNanos);
        queueLatencyMillisHistogram.addObservation(queueLatencyMillis);

        if (trackMaxQueueLatency) {
            maxQueueLatencyMillisSinceLastPoll.accumulate(queueLatencyMillis);
        }
    }

    void taskExecutionTime(long taskExecutionNanos) {
        assert taskExecutionNanos >= 0;
        executionEWMA.addValue(taskExecutionNanos);
        totalExecutionTime.add(taskExecutionNanos);
    }

    // Used for testing
    double getExecutionEwmaAlpha() {
        return executionEWMA.getAlpha();
    }

    // Used for testing
    boolean trackingMaxQueueLatency() {
        return trackMaxQueueLatency;
    }

    void appendTaskExecutionDetails(StringBuilder sb) {
        sb.append("task execution EWMA = ")
            .append(TimeValue.timeValueNanos((long) getTaskExecutionEWMA()))
            .append(", ")
            .append("total task execution time = ")
            .append(TimeValue.timeValueNanos(getTotalTaskExecutionTime()))
            .append(", ");
    }

    /**
     * Supports periodic polling for thread pool utilization. Tracks state since the last polling request so that the average utilization
     * since the last poll can be calculated for the next polling request.
     *
     * Uses the difference of {@link #totalExecutionTime} since the last polling request to determine how much activity has occurred.
     */
    private class UtilizationTracker {
        long lastPollTime = System.nanoTime();
        long lastTotalExecutionTime = 0;

        public synchronized double pollUtilization() {
            final long currentTotalExecutionTimeNanos = totalExecutionTime.sum();
            final long currentPollTimeNanos = System.nanoTime();

            final long totalExecutionTimeSinceLastPollNanos = currentTotalExecutionTimeNanos - lastTotalExecutionTime;
            final long timeSinceLastPoll = currentPollTimeNanos - lastPollTime;

            final long maximumExecutionTimeSinceLastPollNanos = timeSinceLastPoll * maximumPoolSize;
            final double utilizationSinceLastPoll = (double) totalExecutionTimeSinceLastPollNanos / maximumExecutionTimeSinceLastPollNanos;

            lastTotalExecutionTime = currentTotalExecutionTimeNanos;
            lastPollTime = currentPollTimeNanos;

            return utilizationSinceLastPoll;
        }
    }
}
