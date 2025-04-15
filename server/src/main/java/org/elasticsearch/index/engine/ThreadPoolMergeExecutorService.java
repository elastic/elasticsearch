/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongUnaryOperator;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.ABORT;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.BACKLOG;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.RUN;

public class ThreadPoolMergeExecutorService {
    // TODO make these settings dynamic
    // TODO highlight that these settings are effective only if "indices.merge.scheduler.use_thread_pool" is "true"
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING = new Setting<>(
            "indices.merge.disk.watermark.high",
            "96%",
            (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.high"),
            new WatermarkValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING = new Setting<>(
            "indices.merge.disk.watermark.high.max_headroom",
            (settings) -> {
                if (INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.exists(settings)) {
                    return "-1";
                } else {
                    return "40GB";
                }
            },
            (s) -> ByteSizeValue.parseBytesSizeValue(s, "indices.merge.disk.watermark.high.max_headroom"),
            new HeadroomValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING = new Setting<>(
            "indices.merge.disk.watermark.flood_stage",
            "98%",
            (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.flood_stage"),
            new WatermarkValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING = new Setting<>(
            "indices.merge.disk.watermark.flood_stage.max_headroom",
            (settings) -> {
                if (INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING.exists(settings)) {
                    return "-1";
                } else {
                    return "20GB";
                }
            },
            (s) -> ByteSizeValue.parseBytesSizeValue(s, "indices.merge.disk.watermark.flood_stage.max_headroom"),
            new HeadroomValidator(),
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
    );

    static class WatermarkValidator implements Setting.Validator<RelativeByteSizeValue> {

        @Override
        public void validate(RelativeByteSizeValue value) {
        }

        @Override
        public void validate(final RelativeByteSizeValue value, final Map<Setting<?>, Object> settings) {
            final RelativeByteSizeValue high = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
            final RelativeByteSizeValue flood = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            if (high.isAbsolute() == false && flood.isAbsolute() == false) { // Validate as percentages
                final double highWatermarkThreshold = high.getRatio().getAsPercent();
                final double floodThreshold = flood.getRatio().getAsPercent();
                if (highWatermarkThreshold > floodThreshold) {
                    throw new IllegalArgumentException(
                            "indices merge high disk watermark ["
                                    + high.getStringRep()
                                    + "] more than flood stage disk watermark ["
                                    + flood.getStringRep()
                                    + "]"
                    );
                }
            } else if (high.isAbsolute() && flood.isAbsolute()) { // Validate as absolute values
                final ByteSizeValue highWatermarkBytes = high.getAbsolute();
                final ByteSizeValue floodStageBytes = flood.getAbsolute();
                if (highWatermarkBytes.getBytes() < floodStageBytes.getBytes()) {
                    throw new IllegalArgumentException(
                            "indices merge high disk watermark ["
                                    + high.getStringRep()
                                    + "] less than flood stage disk watermark ["
                                    + flood.getStringRep()
                                    + "]"
                    );
                }
            } else {
                final String message = Strings.format(
                        "unable to consistently parse [%s=%s], and [%s=%s] as percentage or bytes",
                        INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(),
                        high.getStringRep(),
                        INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(),
                        flood.getStringRep()
                );
                throw new IllegalArgumentException(message);
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            List<Setting<?>> res = List.of(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING, INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            return res.iterator();
        }
    }


    /**
     * Validates that high and flood stage max headrooms adhere to the comparison: high &lt; low.
     * Also validates that if max headrooms are set, the respective watermark values should be ratios/percentages.
     * Also validates that if the high max headroom is set, then the flood stage max headroom must be set as well.
     * Else, throws an exception.
     */
    static class HeadroomValidator implements Setting.Validator<ByteSizeValue> {

        @Override
        public void validate(ByteSizeValue value) {

        }

        @Override
        public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings, boolean isPresent) {
            if (isPresent && value.equals(ByteSizeValue.MINUS_ONE)) {
                throw new IllegalArgumentException("setting a headroom value to less than 0 is not supported");
            }

            final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING);
            final ByteSizeValue floodHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING);

            // Ensure that if max headroom values are set, then watermark values are ratios/percentages.
            final RelativeByteSizeValue high = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
            final RelativeByteSizeValue flood = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING);
            if ((high.isAbsolute() || flood.isAbsolute())
                && (highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false || floodHeadroom.equals(ByteSizeValue.MINUS_ONE) == false)) {
                throw new IllegalArgumentException(
                    "At least one of the indices merge disk max headroom settings is set [high="
                        + highHeadroom.getStringRep()
                        + ", flood="
                        + floodHeadroom.getStringRep()
                        + "], while the indices merge disk watermark values are set to absolute values "
                        + "instead of ratios/percentages [high="
                        + high.getStringRep()
                        + ", flood="
                        + flood.getStringRep()
                        + "]"
                );
            }

            if (highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false && floodHeadroom.equals(ByteSizeValue.MINUS_ONE)) {
                throw new IllegalArgumentException(
                        "indices merge flood disk max headroom ["
                                + floodHeadroom.getStringRep()
                                + "] is not set, while the indices merge high disk max headroom is set ["
                                + highHeadroom.getStringRep()
                                + "]"
                );
            }

            // For the comparisons, we need to mind that headroom values can default to -1.
            if (floodHeadroom.compareTo(highHeadroom) > 0 && highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false) {
                throw new IllegalArgumentException(
                        "indices merge flood disk max headroom ["
                                + floodHeadroom.getStringRep()
                                + "] more than indices merge high disk max headroom ["
                                + highHeadroom.getStringRep()
                                + "]"
                );
            }
        }

        @Override
        public Iterator<Setting<?>> settings() {
            List<Setting<?>> res = List.of(
                INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
                INDICES_MERGE_DISK_FLOOD_STAGE_WATERMARK_SETTING,
                INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
                INDICES_MERGE_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING
            );
            return res.iterator();
        }
    }

    /**
     * Floor for IO write rate limit of individual merge tasks (we will never go any lower than this)
     */
    static final ByteSizeValue MIN_IO_RATE = ByteSizeValue.ofMb(5L);
    /**
     * Ceiling for IO write rate limit of individual merge tasks (we will never go any higher than this)
     */
    static final ByteSizeValue MAX_IO_RATE = ByteSizeValue.ofMb(10240L);
    /**
     * Initial value for IO write rate limit of individual merge tasks when doAutoIOThrottle is true
     */
    static final ByteSizeValue START_IO_RATE = ByteSizeValue.ofMb(20L);
    /**
     * Total number of submitted merge tasks that support IO auto throttling and that have not yet been run (or aborted).
     * This includes merge tasks that are currently running and that are backlogged (by their respective merge schedulers).
     */
    private final AtomicInteger ioThrottledMergeTasksCount = new AtomicInteger();
    /**
     * The merge tasks that are waiting execution. This does NOT include backlogged or currently executing merge tasks.
     * For instance, this can be empty while there are backlogged merge tasks awaiting re-enqueuing.
     */
    private final PriorityBlockingQueue<MergeTask> queuedMergeTasks = new PriorityBlockingQueue<>(
        64,
        Comparator.comparingLong(MergeTask::estimatedMergeSize)
    );
    /**
     * The set of all merge tasks currently being executed by merge threads from the pool.
     * These are tracked notably in order to be able to update their disk IO throttle rate, after they have started, while executing.
     */
    private final Set<MergeTask> runningMergeTasks = ConcurrentCollections.newConcurrentSet();
    /**
     * Current IO write throttle rate, in bytes per sec, that's in effect for all currently running merge tasks,
     * across all {@link ThreadPoolMergeScheduler}s that use this instance of the queue.
     */
    private final AtomicIORate targetIORateBytesPerSec = new AtomicIORate(START_IO_RATE.getBytes());
    private final ExecutorService executorService;
    /**
     * The maximum number of concurrently running merges, given the number of threads in the pool.
     */
    private final int maxConcurrentMerges;
    private final int concurrentMergesFloorLimitForThrottling;
    private final int concurrentMergesCeilLimitForThrottling;

    public static @Nullable ThreadPoolMergeExecutorService maybeCreateThreadPoolMergeExecutorService(
        ThreadPool threadPool,
        Settings settings
    ) {
        if (ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.get(settings)) {
            return new ThreadPoolMergeExecutorService(threadPool);
        } else {
            return null;
        }
    }

    private ThreadPoolMergeExecutorService(ThreadPool threadPool) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
        // the intent here is to throttle down whenever we submit a task and no other task is running
        this.concurrentMergesFloorLimitForThrottling = 2;
        this.concurrentMergesCeilLimitForThrottling = maxConcurrentMerges * 2;
        assert concurrentMergesFloorLimitForThrottling <= concurrentMergesCeilLimitForThrottling;
    }

    boolean submitMergeTask(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        // first enqueue the runnable that runs exactly one merge task (the smallest it can find)
        if (enqueueMergeTaskExecution() == false) {
            // if the thread pool cannot run the merge, just abort it
            mergeTask.abort();
            return false;
        } else {
            if (mergeTask.supportsIOThrottling()) {
                // count enqueued merge tasks that support IO auto throttling, and maybe adjust IO rate for all
                int currentTaskCount = ioThrottledMergeTasksCount.incrementAndGet();
                targetIORateBytesPerSec.update(
                    currentTargetIORateBytesPerSec -> newTargetIORateBytesPerSec(
                        currentTargetIORateBytesPerSec,
                        currentTaskCount,
                        concurrentMergesFloorLimitForThrottling,
                        concurrentMergesCeilLimitForThrottling
                    ),
                    (prevTargetIORateBytesPerSec, newTargetIORateBytesPerSec) -> {
                        // it's OK to have this method update merge tasks concurrently, with different targetMBPerSec values,
                        // as it's not important that all merge tasks are throttled to the same IO rate at all time.
                        // For performance reasons, we don't synchronize the updates to targetMBPerSec values with the update of running
                        // merges.
                        if (prevTargetIORateBytesPerSec != newTargetIORateBytesPerSec) {
                            runningMergeTasks.forEach(runningMergeTask -> {
                                if (runningMergeTask.supportsIOThrottling()) {
                                    runningMergeTask.setIORateLimit(newTargetIORateBytesPerSec);
                                }
                            });
                        }
                    }
                );
            }
            // then enqueue the merge task proper
            queuedMergeTasks.add(mergeTask);
            return true;
        }
    }

    void reEnqueueBackloggedMergeTask(MergeTask mergeTask) {
        queuedMergeTasks.add(mergeTask);
    }

    public boolean allDone() {
        return queuedMergeTasks.isEmpty() && runningMergeTasks.isEmpty() && ioThrottledMergeTasksCount.get() == 0L;
    }

    /**
     * Enqueues a runnable that executes exactly one merge task, the smallest that is runnable at some point in time.
     * A merge task is not runnable if its scheduler already reached the configured max-allowed concurrency level.
     */
    private boolean enqueueMergeTaskExecution() {
        try {
            executorService.execute(() -> {
                // one such runnable always executes a SINGLE merge task from the queue
                // this is important for merge queue statistics, i.e. the executor's queue size represents the current amount of merges
                while (true) {
                    MergeTask smallestMergeTask;
                    try {
                        // will block if there are backlogged merges until they're enqueued again
                        smallestMergeTask = queuedMergeTasks.take();
                    } catch (InterruptedException e) {
                        // An active worker thread has been interrupted while waiting for backlogged merges to be re-enqueued.
                        // In this case, we terminate the worker thread promptly and forget about the backlogged merges.
                        // It is OK to forget about merges in this case, because active worker threads are only interrupted
                        // when the node is shutting down, in which case in-memory accounting of merging activity is not relevant.
                        // As part of {@link java.util.concurrent.ThreadPoolExecutor#shutdownNow()} the thread pool's work queue
                        // is also drained, so any queued merge tasks are also forgotten.
                        break;
                    }
                    // let the task's scheduler decide if it can actually run the merge task now
                    ThreadPoolMergeScheduler.Schedule schedule = smallestMergeTask.schedule();
                    if (schedule == RUN) {
                        runMergeTask(smallestMergeTask);
                        break;
                    } else if (schedule == ABORT) {
                        abortMergeTask(smallestMergeTask);
                        break;
                    } else {
                        assert schedule == BACKLOG;
                        // the merge task is backlogged by the merge scheduler, try to get the next smallest one
                        // it's then the duty of the said merge scheduler to re-enqueue the backlogged merge task when it can be run
                    }
                }
            });
            return true;
        } catch (Throwable t) {
            // cannot execute merges because the executor is shutting down
            assert t instanceof RejectedExecutionException;
            return false;
        }
    }

    private void runMergeTask(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        boolean added = runningMergeTasks.add(mergeTask);
        assert added : "starting merge task [" + mergeTask + "] registered as already running";
        try {
            if (mergeTask.supportsIOThrottling()) {
                mergeTask.setIORateLimit(targetIORateBytesPerSec.get());
            }
            mergeTask.run();
        } finally {
            boolean removed = runningMergeTasks.remove(mergeTask);
            assert removed : "completed merge task [" + mergeTask + "] not registered as running";
            if (mergeTask.supportsIOThrottling()) {
                ioThrottledMergeTasksCount.decrementAndGet();
            }
        }
    }

    private void abortMergeTask(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        assert runningMergeTasks.contains(mergeTask) == false;
        try {
            mergeTask.abort();
        } finally {
            if (mergeTask.supportsIOThrottling()) {
                ioThrottledMergeTasksCount.decrementAndGet();
            }
        }
    }

    private static long newTargetIORateBytesPerSec(
        long currentTargetIORateBytesPerSec,
        int currentlySubmittedIOThrottledMergeTasks,
        int concurrentMergesFloorLimitForThrottling,
        int concurrentMergesCeilLimitForThrottling
    ) {
        final long newTargetIORateBytesPerSec;
        if (currentlySubmittedIOThrottledMergeTasks < concurrentMergesFloorLimitForThrottling
            && currentTargetIORateBytesPerSec > MIN_IO_RATE.getBytes()) {
            // decrease target IO rate by 10% (capped)
            newTargetIORateBytesPerSec = Math.max(
                MIN_IO_RATE.getBytes(),
                currentTargetIORateBytesPerSec - currentTargetIORateBytesPerSec / 10L
            );
        } else if (currentlySubmittedIOThrottledMergeTasks > concurrentMergesCeilLimitForThrottling
            && currentTargetIORateBytesPerSec < MAX_IO_RATE.getBytes()) {
                // increase target IO rate by 20% (capped)
                newTargetIORateBytesPerSec = Math.min(
                    MAX_IO_RATE.getBytes(),
                    currentTargetIORateBytesPerSec + currentTargetIORateBytesPerSec / 5L
                );
            } else {
                newTargetIORateBytesPerSec = currentTargetIORateBytesPerSec;
            }
        return newTargetIORateBytesPerSec;
    }

    static class AtomicIORate {
        private final AtomicLong ioRate;

        AtomicIORate(long initialIORate) {
            ioRate = new AtomicLong(initialIORate);
        }

        long get() {
            return ioRate.get();
        }

        // Exactly like {@link AtomicLong#updateAndGet} but calls the consumer rather than return the new (updated) value.
        // The consumer receives both the previous and the updated values (which can be equal).
        void update(LongUnaryOperator updateFunction, AtomicIORate.UpdateConsumer updateConsumer) {
            long prev = ioRate.get(), next = 0L;
            for (boolean haveNext = false;;) {
                if (haveNext == false) next = updateFunction.applyAsLong(prev);
                if (ioRate.weakCompareAndSetVolatile(prev, next)) {
                    updateConsumer.accept(prev, next);
                    return;
                }
                haveNext = (prev == (prev = ioRate.get()));
            }
        }

        @FunctionalInterface
        interface UpdateConsumer {
            void accept(long prev, long next);
        }
    }

    public boolean usingMaxTargetIORateBytesPerSec() {
        return MAX_IO_RATE.getBytes() == targetIORateBytesPerSec.get();
    }

    // exposed for tests
    Set<MergeTask> getRunningMergeTasks() {
        return runningMergeTasks;
    }

    // exposed for tests
    PriorityBlockingQueue<MergeTask> getQueuedMergeTasks() {
        return queuedMergeTasks;
    }

    // exposed for tests and stats
    long getTargetIORateBytesPerSec() {
        return targetIORateBytesPerSec.get();
    }

    // exposed for tests
    int getMaxConcurrentMerges() {
        return maxConcurrentMerges;
    }
}
