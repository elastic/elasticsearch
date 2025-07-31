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

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.concurrent.atomic.LongAdder;
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
        this.framedTimeTracker = new FramedTimeTracker(
            trackingConfig.utilizationReportingInterval(),
            trackingConfig.utilizationSamplingInterval()
        );
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
     * Returns thread-pool utilization from last completed time interval(frame) {@link TaskTrackingConfig#utilizationReportingInterval()}.
     * Utilization is measured as {@code all-threads-total-execution-time / (total-thread-count * interval)}.
     * This metric is updated once per interval, and returns last completed measurement. For example:
     * if interval is 30 seconds, at clock time 00:30-01:00 it will return utilization from 00:00-00:30.
     * There is no synchronization with clocks and system time.
     *
     * If caller needs longer intervals it should poll on every tracker-interval and aggregate on it's own. Another option is to extend
     * framedTimeTracker to remember multiple past frames, and return aggregated view from here.
     */
    public double utilization() {
        return (double) framedTimeTracker.totalTime() / (double) getMaximumPoolSize() / (double) framedTimeTracker.reportingInterval();
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
     * Tracks threads execution in continuous, non-overlapping, and even time frames.
     */
    public static class FramedTimeTracker {
        private final long reportingInterval;
        private final long frameDuration;
        private final Supplier<Long> timeNow;
        private final AtomicReference<FrameWindow> frameWindowRef;
        private final AtomicBoolean updatingFrame = new AtomicBoolean();
        private final AtomicLong currentFrameNum = new AtomicLong();

        // for testing
        public FramedTimeTracker(long reportingInterval, long frameDuration, Supplier<Long> timeNow) {
            assert reportingInterval / frameDuration > 0;
            this.reportingInterval = reportingInterval;
            this.frameDuration = frameDuration;
            this.timeNow = timeNow;
            this.frameWindowRef = new AtomicReference<>(FrameWindow.empty((int) (reportingInterval/frameDuration)));
        }

        FramedTimeTracker(Duration reportingInterval, Duration frameInterval) {
            this(
                reportingInterval.toNanos(),
                frameInterval.toNanos(),
                System::nanoTime
            );
        }

        public long reportingInterval() {
            return reportingInterval;
        }

        /**
         * Returns current FrameWindow. If window is stale, it will slide to current time.
         * @param now - current frame
         */
        private FrameWindow getWindow(long now) {
            var current = currentFrameNum.get();
            // first time in new frame
            if (current < now) {
                // only one thread will perform frame update, others spinWait
                if (updatingFrame.compareAndSet(false, true)) {
                    final var moveOffset = now - current;
                    final var newWindow = frameWindowRef.get().moveBy(moveOffset);
                    frameWindowRef.set(newWindow);
                    currentFrameNum.set(now);
                    updatingFrame.set(false);
                } else {
                    while (updatingFrame.get()) {
                        Thread.onSpinWait();
                    }
                    // an edge case when all the following happen:
                    // 1. window was stale, at least 1 frame
                    // 2. two or more threads try to update window
                    // 3. it's happening at the end of the frame, beginning new frame
                    // for example, lets say interval is 10
                    // and there are two concurrent calls getWindow(9)->frame0 and getWindow(10)->frame1
                    // both need to update window, but those are different windows,
                    // two things might happen:
                    // 1. getWindow(9) updates window and uses it, but getWindow(10) need to update window again
                    // 2. getWindow(10) updates window, then getWindow(9) will see a newer window, so we record task in a newer frame,
                    // basically rounding-up frame when it's happening.
                    if (currentFrameNum.get() < now) {
                        return getWindow(now);
                    }
                }
            }
            return frameWindowRef.get();
        }

        /**
         * Start tracking new task, assume that task runs indefinitely, or at least till end of frame.
         * If task finishes sooner than end of interval {@link FramedTimeTracker#endTask()} will deduct remaining time.
         */
        public void startTask() {
            final var nowTime = timeNow.get();
            final var now = nowTime / frameDuration;
            final var frameWindow = getWindow(now);
            frameWindow.frames[0].ongoingTasks.increment();
            frameWindow.frames[0].startEndDiff.add((now + 1) * frameDuration - nowTime);
        }

        /**
         * Stop task tracking. We already assumed that task runs till end of frame, here we deduct not used time.
         */
        public void endTask() {
            final var nowTime = timeNow.get();
            final var now = nowTime / frameDuration;
            final var frameWindow = getWindow(now);
            frameWindow.frames[0].ongoingTasks.decrement();
            frameWindow.frames[0].startEndDiff.add(-((now + 1) * frameDuration - nowTime));
        }

        /**
         * Returns total execution time from last interval.
         */
        public long totalTime() {
            final var now = timeNow.get() / frameDuration;
            final var frameWindow = getWindow(now);
            // total time is sum of ongoing tasks in frame N-1 and all starts and ends in N frame
            // so for the previous frame (now-1), it would be (now-2) ongoing tasks + (now -1) start/end tasks
            var totalTime = 0L;
            for (var i = 1; i < frameWindow.frames.length - 1; i++) { // first and last frames are not used, see FrameWindow description
                final var ongoingTasks = frameWindow.frames[i + 1].ongoingTasks.sum();
                final var startEndDiff = frameWindow.frames[i].startEndDiff.sum();
                totalTime += ongoingTasks * frameDuration + startEndDiff;
            }
            return totalTime;
        }

        /**
         * A single frame that tracks how many tasks are still running at the end of frame
         * and diffs from task start and end.
         */
        record Frame(LongAdder ongoingTasks, LongAdder startEndDiff) {
            Frame() {
                this(new LongAdder(), new LongAdder());
            }
        }

        /**
         * A frame window of consecutive frames. frames[0] is now, frames[1] is now-1.
         * To calculate frame time usage we need to know ongoing-tasks from previous frame and all starts and ends in current frame.
         * That means we cannot calculate time for first and last frames in the array. First one is in-progress, last one has no information
         * about previous ongoing tasks. So completed frames are between 1..length-2.
         */
        record FrameWindow(Frame[] frames) {

            static FrameWindow empty(int size) {
                // first and last frames are incomplete, adding two more frames
                final var frames = new Frame[size + 2];
                for (var i = 0; i < frames.length; i++) {
                    frames[i] = new Frame();
                }
                return new FrameWindow(frames);
            }

            /**
             * Creates a new window by sliding current by moveFrames. If new window overlaps with current window Frames are reused.
             * So there is no risk of losing data when start/endTask updates counters in the past window frame.
             */
            FrameWindow moveBy(long moveFrames) {
                if (moveFrames == 0) {
                    return this;
                }
                final var newFramesNum = (int) Math.min(frames.length, moveFrames); // non-overlapping frames with current window
                final var newFrames = new Frame[frames.length];
                // copy overlapping frames to the end of array
                System.arraycopy(frames, 0, newFrames, newFramesNum, frames.length - newFramesNum);
                // initialize new frames in the beginning of array
                // a new frame always starts with last known ongoing tasks
                final var ongoingTasks = frames[0].ongoingTasks.sum();
                for (var i=0; i<newFramesNum; i++) {
                    final var frame = new Frame();
                    frame.ongoingTasks.add(ongoingTasks);
                    newFrames[i] = frame;
                }
                return new FrameWindow(newFrames);
            }
        }
    }
}
