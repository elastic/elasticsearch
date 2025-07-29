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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_QUEUE_TIME;
import static org.elasticsearch.threadpool.ThreadPool.THREAD_POOL_METRIC_NAME_UTILIZATION;

/**
 * An extension to thread pool executor, which tracks statistics for the task execution time.
 */
public final class TaskExecutionTimeTrackingEsThreadPoolExecutor extends EsThreadPoolExecutor {
    public static final int QUEUE_LATENCY_HISTOGRAM_BUCKETS = 18;
    private static final int[] LATENCY_PERCENTILES_TO_REPORT = { 50, 90, 99 };

    private final Function<Runnable, WrappedRunnable> runnableWrapper;
    private final ExponentiallyWeightedMovingAverage executionEWMA;
    private final LongAdder totalExecutionTime = new LongAdder();
    // The set of currently running tasks and the timestamp of when they started execution in the Executor.
    private final Map<Runnable, Long> ongoingTasks = new ConcurrentHashMap<>();
    private final ExponentialBucketHistogram queueLatencyMillisHistogram = new ExponentialBucketHistogram(QUEUE_LATENCY_HISTOGRAM_BUCKETS);
    private final TaskTrackingConfig trackingConfig;
    private final FramedTimeTracker framedTimeTracker;
    private LongAccumulator maxQueueLatencyMillisSinceLastPoll = new LongAccumulator(Long::max, 0);

    public TaskExecutionTimeTrackingEsThreadPoolExecutor(
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
        this.executionEWMA = new ExponentiallyWeightedMovingAverage(trackingConfig.executionTimeEwmaAlpha(), 0);
        this.trackingConfig = trackingConfig;
        this.framedTimeTracker = new FramedTimeTracker(trackingConfig.utilizationInterval().toNanos());
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
                () -> new DoubleWithAttributes(utilization(), Map.of())
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
        if (trackingConfig.trackMaxQueueLatency() == false) {
            return 0;
        }
        return maxQueueLatencyMillisSinceLastPoll.getThenReset();
    }

    public TaskTrackingConfig trackingConfig() {
        return trackingConfig;
    }

    /**
     * Returns thread-pool utilization from last completed time interval(frame) {@link TaskTrackingConfig#utilizationInterval()}.
     * Utilization is measured as {@code all-threads-total-execution-time / (total-thread-count * interval)}.
     * This metric is updated once per interval, and returns last completed measurement. For example:
     * if interval is 30 seconds, at clock time 00:30-01:00 it will return utilization from 00:00-00:30.
     * Thou there is no synchronization with clocks and system time.
     *
     * If caller needs longer intervals it should poll on every tracker-interval and aggregate on it's own. Another option is to extend
     * framedTimeTracker to remember multiple past frames, and return aggregated view from here.
     */
    public double utilization() {
        return (double) framedTimeTracker.previousFrameTime() / (double) getMaximumPoolSize() / (double) framedTimeTracker.interval;
    }

    @Override
    protected void beforeExecute(Thread t, Runnable r) {
        if (trackingConfig.trackOngoingTasks()) {
            ongoingTasks.put(r, System.nanoTime());
        }

        assert super.unwrap(r) instanceof TimedRunnable : "expected only TimedRunnables in queue";
        final TimedRunnable timedRunnable = (TimedRunnable) super.unwrap(r);
        timedRunnable.beforeExecute();
        final long taskQueueLatency = timedRunnable.getQueueTimeNanos();
        assert taskQueueLatency >= 0;
        var queueLatencyMillis = TimeUnit.NANOSECONDS.toMillis(taskQueueLatency);
        queueLatencyMillisHistogram.addObservation(queueLatencyMillis);

        if (trackingConfig.trackMaxQueueLatency()) {
            maxQueueLatencyMillisSinceLastPoll.accumulate(queueLatencyMillis);
        }
        if (trackingConfig.trackUtilization()) {
            framedTimeTracker.startTask();
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
            if (trackingConfig.trackUtilization()) {
                framedTimeTracker.endTask();
            }
        } finally {
            // if trackOngoingTasks is false -> ongoingTasks must be empty
            assert trackingConfig.trackOngoingTasks() || ongoingTasks.isEmpty();
            if (trackingConfig.trackOngoingTasks()) {
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
        return trackingConfig.trackOngoingTasks() ? Map.copyOf(ongoingTasks) : Map.of();
    }

    // Used for testing
    public double getExecutionEwmaAlpha() {
        return executionEWMA.getAlpha();
    }

    // Used for testing
    public boolean trackingMaxQueueLatency() {
        return trackingConfig.trackMaxQueueLatency();
    }

    /**
     * Tracks treads execution in continuous, non-overlapping, and even time frames. Provides accurate total execution time measurement
     * for past frames, specifically previous frame (now - 1 frame) to measure utilization.
     *
     * Can be extended to remember multiple past frames.
     */
    public static class FramedTimeTracker {
        private final long interval;
        private final Supplier<Long> timeNow;
        private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
        private final AtomicLong ongoingTasks = new AtomicLong();
        private final AtomicLong currentFrame = new AtomicLong();
        private final AtomicLong currentTime = new AtomicLong();
        private final AtomicLong previousTime = new AtomicLong();

        // for testing
        public FramedTimeTracker(long intervalNano, Supplier<Long> timeNow) {
            assert intervalNano > 0;
            this.interval = intervalNano;
            this.timeNow = timeNow;
        }

        FramedTimeTracker(long intervalNano) {
            assert intervalNano > 0;
            this.interval = intervalNano;
            this.timeNow = System::nanoTime;
        }

        public long interval() {
            return interval;
        }

        /**
         * Update frames to current time. There are no guaranties that it will be invoked frequently.
         * For example when there are no tasks and no requests for previousFrameTime.
         *
         * When it's invoked frequently, at least once per frame, we move currentTime into previousTime.
         * That concludes currentTime and it's accurate.
         *
         * When it's invoked infrequently, once in multiple frames, current and previous frames are going to be stale.
         * Which is ok, that means there were no changes in tasks(start/end), all ongoing tasks are still running.
         * That means ongoing tasks fully utilized previous frames. And we can accurately tell previous frame usage.
         */
        private void updateFrame0(long nowTime) {
            var now = nowTime / interval;
            var current = currentFrame.get();
            if (current < now) {
                rwlock.readLock().unlock();
                rwlock.writeLock().lock();
                current = currentFrame.get(); // make sure it didnt change during lock acquisition
                if (current < now) {
                    var tasks = ongoingTasks.get();
                    if (current == now - 1) {
                        previousTime.set(currentTime.get());
                    } else {
                        previousTime.set(tasks * interval);
                    }
                    currentTime.set(tasks * interval);
                    currentFrame.set(now);
                }
                rwlock.readLock().lock();
                rwlock.writeLock().unlock();
            }
        }

        /**
         * Start tracking new task, assume that task runs indefinitely, or at least till end of frame.
         * If task finishes sooner than end of interval {@link FramedTimeTracker#endTask()} will deduct remaining time.
         */
        public void startTask() {
            rwlock.readLock().lock();
            var now = timeNow.get();
            updateFrame0(now);
            ongoingTasks.incrementAndGet();
            currentTime.updateAndGet((t) -> t + (currentFrame.get() + 1) * interval - now);
            rwlock.readLock().unlock();
        }

        /**
         * Stop task tracking. We already assumed that task runs till end of frame, here we deduct not used time.
         */
        public void endTask() {
            rwlock.readLock().lock();
            var now = timeNow.get();
            updateFrame0(now);
            ongoingTasks.decrementAndGet();
            currentTime.updateAndGet((t) -> t - (currentFrame.get() + 1) * interval + now);
            rwlock.readLock().unlock();
        }

        /**
         * Returns previous frame total execution time.
         */
        public long previousFrameTime() {
            try {
                rwlock.readLock().lock();
                updateFrame0(timeNow.get());
                return previousTime.get();
            } finally {
                rwlock.readLock().unlock();
            }
        }
    }
}
