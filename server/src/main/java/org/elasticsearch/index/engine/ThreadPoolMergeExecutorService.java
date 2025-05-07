/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.engine.ThreadPoolMergeScheduler.MergeTask;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongUnaryOperator;
import java.util.function.ToLongFunction;

import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.ABORT;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.BACKLOG;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.Schedule.RUN;
import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.monitor.fs.FsProbe.getFSInfo;

public class ThreadPoolMergeExecutorService implements Closeable {
    /** How frequently we check disk usage (default: 5 seconds). */
    public static final Setting<TimeValue> INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING = Setting.positiveTimeSetting(
        "indices.merge.disk.check_interval",
        TimeValue.timeValueSeconds(5),
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<RelativeByteSizeValue> INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING = new Setting<>(
        "indices.merge.disk.watermark.high",
        CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING,
        (s) -> RelativeByteSizeValue.parseRelativeByteSizeValue(s, "indices.merge.disk.watermark.high"),
        new Setting.Validator<>() {
            @Override
            public void validate(RelativeByteSizeValue value) {}

            @Override
            public void validate(RelativeByteSizeValue value, Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent && settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                    throw new IllegalArgumentException(
                        "indices merge watermark setting is only effective when ["
                            + USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey()
                            + "] is set to [true]"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> res = List.of(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING, USE_THREAD_POOL_MERGE_SCHEDULER_SETTING);
                return res.iterator();
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING = new Setting<>(
        "indices.merge.disk.watermark.high.max_headroom",
        (settings) -> {
            if (INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.exists(settings)) {
                return "-1";
            } else {
                return CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.get(settings).toString();
            }
        },
        (s) -> ByteSizeValue.parseBytesSizeValue(s, "indices.merge.disk.watermark.high.max_headroom"),
        new Setting.Validator<>() {
            @Override
            public void validate(ByteSizeValue value) {}

            @Override
            public void validate(final ByteSizeValue value, final Map<Setting<?>, Object> settings, boolean isPresent) {
                if (isPresent) {
                    if (value.equals(ByteSizeValue.MINUS_ONE)) {
                        throw new IllegalArgumentException("setting a headroom value to less than 0 is not supported");
                    }
                    if (settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                        throw new IllegalArgumentException(
                            "indices merge max headroom setting is only effective when ["
                                + USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey()
                                + "] is set to [true]"
                        );
                    }
                }
                final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING);
                final RelativeByteSizeValue highWatermark = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
                if (highWatermark.isAbsolute() && highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false) {
                    throw new IllegalArgumentException(
                        "indices merge max headroom setting is set, but disk watermark value is not a relative value"
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                List<Setting<?>> res = List.of(
                    INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
                    INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
                    USE_THREAD_POOL_MERGE_SCHEDULER_SETTING
                );
                return res.iterator();
            }
        },
        Property.Dynamic,
        Property.NodeScope
    );
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
    private final PriorityBlockingQueueWithBudget<MergeTask> queuedMergeTasks = new PriorityBlockingQueueWithBudget<>(
        MergeTask::estimatedRemainingMergeSize,
        Long.MAX_VALUE
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
    private final Scheduler.Cancellable diskSpaceMonitor;

    private final List<MergeEventListener> mergeEventListeners = new CopyOnWriteArrayList<>();

    public static @Nullable ThreadPoolMergeExecutorService maybeCreateThreadPoolMergeExecutorService(
        ThreadPool threadPool,
        Settings settings,
        NodeEnvironment nodeEnvironment
    ) {
        if (ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.get(settings)) {
            return new ThreadPoolMergeExecutorService(threadPool, settings, nodeEnvironment);
        } else {
            return null;
        }
    }

    private ThreadPoolMergeExecutorService(ThreadPool threadPool, Settings settings, NodeEnvironment nodeEnvironment) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
        // the intent here is to throttle down whenever we submit a task and no other task is running
        this.concurrentMergesFloorLimitForThrottling = 2;
        this.concurrentMergesCeilLimitForThrottling = maxConcurrentMerges * 2;
        assert concurrentMergesFloorLimitForThrottling <= concurrentMergesCeilLimitForThrottling;
        this.diskSpaceMonitor = threadPool.scheduleWithFixedDelay(
            new DiskSpaceMonitor(
                INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.get(settings),
                INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING.get(settings),
                nodeEnvironment.dataPaths()
            ),
            INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.get(settings),
            threadPool.generic()
        );
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
            enqueueMergeTask(mergeTask);
            return true;
        }
    }

    void reEnqueueBackloggedMergeTask(MergeTask mergeTask) {
        enqueueMergeTask(mergeTask);
    }

    private void enqueueMergeTask(MergeTask mergeTask) {
        // To ensure that for a given merge onMergeQueued is called before onMergeAborted or onMergeCompleted, we call onMergeQueued
        // before adding the merge task to the queue. Adding to the queue should not fail.
        mergeEventListeners.forEach(l -> l.onMergeQueued(mergeTask.getOnGoingMerge(), mergeTask.getMergeMemoryEstimateBytes()));
        boolean added = queuedMergeTasks.enqueue(mergeTask);
        assert added;
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
                    PriorityBlockingQueueWithBudget<MergeTask>.ElementWithReleasableBudget smallestMergeTaskWithReleasableBudget;
                    try {
                        // will block if there are backlogged merges until they're enqueued again
                        smallestMergeTaskWithReleasableBudget = queuedMergeTasks.take();
                    } catch (InterruptedException e) {
                        // An active worker thread has been interrupted while waiting for backlogged merges to be re-enqueued.
                        // In this case, we terminate the worker thread promptly and forget about the backlogged merges.
                        // It is OK to forget about merges in this case, because active worker threads are only interrupted
                        // when the node is shutting down, in which case in-memory accounting of merging activity is not relevant.
                        // As part of {@link java.util.concurrent.ThreadPoolExecutor#shutdownNow()} the thread pool's work queue
                        // is also drained, so any queued merge tasks are also forgotten.
                        break;
                    }
                    try {
                        // let the task's scheduler decide if it can actually run the merge task now
                        MergeTask smallestMergeTask = smallestMergeTaskWithReleasableBudget.element();
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
                    } finally {
                        smallestMergeTaskWithReleasableBudget.close();
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
            mergeEventListeners.forEach(l -> l.onMergeCompleted(mergeTask.getOnGoingMerge()));
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
            mergeEventListeners.forEach(l -> l.onMergeAborted(mergeTask.getOnGoingMerge()));
        }
    }

    class DiskSpaceMonitor implements Runnable {
        private static final Logger LOGGER = LogManager.getLogger(ThreadPoolMergeExecutorService.DiskSpaceMonitor.class);
        private final RelativeByteSizeValue highStageWatermark;
        private final ByteSizeValue highStageMaxHeadroom;
        private final NodeEnvironment.DataPath[] dataPaths;

        DiskSpaceMonitor(
            RelativeByteSizeValue highStageWatermark,
            ByteSizeValue highStageMaxHeadroom,
            NodeEnvironment.DataPath[] dataPaths
        ) {
            this.highStageWatermark = highStageWatermark;
            this.highStageMaxHeadroom = highStageMaxHeadroom;
            this.dataPaths = dataPaths;
        }

        @Override
        public void run() {
            FsInfo.Path mostAvailablePath = null;
            IOException fsInfoException = null;
            for (NodeEnvironment.DataPath dataPath : dataPaths) {
                try {
                    FsInfo.Path fsInfo = getFSInfo(dataPath); // uncached
                    if (mostAvailablePath == null || mostAvailablePath.getAvailable().getBytes() < fsInfo.getAvailable().getBytes()) {
                        mostAvailablePath = fsInfo;
                    }
                } catch (IOException e) {
                    if (fsInfoException == null) {
                        fsInfoException = e;
                    } else {
                        fsInfoException.addSuppressed(e);
                    }
                }
            }
            if (fsInfoException != null) {
                LOGGER.warn("unexpected exception reading filesystem info", fsInfoException);
            }
            if (mostAvailablePath == null) {
                LOGGER.error("Cannot read filesystem info for node data paths " + Arrays.toString(dataPaths));
                return;
            }
            long mostAvailableDiskSpaceBytes = mostAvailablePath.getAvailable().getBytes();
            // subtract the configured free disk space threshold
            mostAvailableDiskSpaceBytes -= getFreeBytesThreshold(mostAvailablePath.getTotal(), highStageWatermark, highStageMaxHeadroom)
                .getBytes();
            long maxMergeSizeLimit = Math.max(0L, mostAvailableDiskSpaceBytes);
            queuedMergeTasks.updateAvailableBudget(maxMergeSizeLimit);
        }

        private static ByteSizeValue getFreeBytesThreshold(
            ByteSizeValue total,
            RelativeByteSizeValue watermark,
            ByteSizeValue maxHeadroom
        ) {
            // If bytes are given, they can be readily returned as free bytes. If percentages are given, we need to calculate the free
            // bytes.
            if (watermark.isAbsolute()) {
                return watermark.getAbsolute();
            }
            return ByteSizeValue.subtract(total, watermark.calculateValue(total, maxHeadroom));
        }
    }

    static class PriorityBlockingQueueWithBudget<E> {
        private final ToLongFunction<? super E> budgetFunction;
        private final PriorityQueue<E> enqueuedByBudget;
        private final IdentityHashMap<E, Long> unreleasedBudgetPerElement;
        private final ReentrantLock lock;
        private final Condition elementAvailable;
        private long availableBudget;

        PriorityBlockingQueueWithBudget(ToLongFunction<? super E> budgetFunction, long availableBudget) {
            this.budgetFunction = budgetFunction;
            this.enqueuedByBudget = new PriorityQueue<>(64, Comparator.comparingLong(budgetFunction));
            this.unreleasedBudgetPerElement = new IdentityHashMap<>();
            this.lock = new ReentrantLock();
            this.elementAvailable = lock.newCondition();
            this.availableBudget = availableBudget;
        }

        boolean enqueue(E e) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                enqueuedByBudget.offer(e);
                elementAvailable.signal();
            } finally {
                lock.unlock();
            }
            return true;
        }

        ElementWithReleasableBudget take() throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                E peek;
                long peekBudget;
                while ((peek = enqueuedByBudget.peek()) == null || (peekBudget = budgetFunction.applyAsLong(peek)) > availableBudget) {
                    elementAvailable.await();
                }
                return new ElementWithReleasableBudget(enqueuedByBudget.poll(), peekBudget);
            } finally {
                lock.unlock();
            }
        }

        void updateAvailableBudget(long availableBudget) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                this.availableBudget = availableBudget;
                // update the per-element budget
                unreleasedBudgetPerElement.replaceAll((e, v) -> budgetFunction.applyAsLong(e));
                // update available budget given the per-element budget
                this.availableBudget -= unreleasedBudgetPerElement.values().stream().reduce(0L, Long::sum);
                elementAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }

        boolean isEmpty() {
            return enqueuedByBudget.isEmpty();
        }

        int size() {
            return enqueuedByBudget.size();
        }

        class ElementWithReleasableBudget implements Releasable {
            private final E element;

            private ElementWithReleasableBudget(E element, long budget) {
                this.element = element;
                assert PriorityBlockingQueueWithBudget.this.lock.isHeldByCurrentThread();
                var prev = unreleasedBudgetPerElement.put(element, budget);
                assert prev == null;
                availableBudget -= budget;
                assert availableBudget >= 0L;
            }

            @Override
            public void close() {
                final ReentrantLock lock = PriorityBlockingQueueWithBudget.this.lock;
                lock.lock();
                try {
                    assert unreleasedBudgetPerElement.containsKey(element);
                    availableBudget += unreleasedBudgetPerElement.remove(element);
                    elementAvailable.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            public E element() {
                return element;
            }
        }
    }

    @Override
    public void close() throws IOException {
        diskSpaceMonitor.cancel();
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

    public void registerMergeEventListener(MergeEventListener consumer) {
        mergeEventListeners.add(consumer);
    }

    // exposed for tests
    Set<MergeTask> getRunningMergeTasks() {
        return runningMergeTasks;
    }

    // exposed for tests
    int getMergeTasksQueueLength() {
        return queuedMergeTasks.size();
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
