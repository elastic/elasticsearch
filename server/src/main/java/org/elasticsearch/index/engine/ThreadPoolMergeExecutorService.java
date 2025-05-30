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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
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
import java.util.function.Consumer;
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
    /**
     * The occupied disk space threshold beyond which NO new merges are started.
     * Conservatively, the estimated temporary disk space required for the to-be-started merge is counted as occupied disk space.
     * Defaults to the routing allocation flood stage limit value (beyond which shards are toggled read-only).
     */
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
    /**
     * The available disk space headroom below which NO new merges are started.
     * Conservatively, the estimated temporary disk space required for the to-be-started merge is NOT counted as available disk space.
     * Defaults to the routing allocation flood stage headroom value (below which shards are toggled read-only),
     * unless the merge occupied disk space threshold is specified, in which case the default headroom value here is unset.
     */
    public static final Setting<ByteSizeValue> INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING = new Setting<>(
        "indices.merge.disk.watermark.high.max_headroom",
        (settings) -> {
            // if the user explicitly set a value for the occupied disk space threshold, disable the implicit headroom value
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
                        throw new IllegalArgumentException(
                            "setting a headroom value to less than 0 is not supported, use [null] value to unset"
                        );
                    }
                    if (settings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING).equals(Boolean.FALSE)) {
                        throw new IllegalArgumentException(
                            "indices merge max headroom setting is only effective when ["
                                + USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey()
                                + "] is set to [true]"
                        );
                    }
                }
                final RelativeByteSizeValue highWatermark = (RelativeByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING);
                final ByteSizeValue highHeadroom = (ByteSizeValue) settings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING);
                if (highWatermark.isAbsolute() && highHeadroom.equals(ByteSizeValue.MINUS_ONE) == false) {
                    throw new IllegalArgumentException(
                        "indices merge max headroom setting is set, but indices merge disk watermark value is not a relative value ["
                            + highWatermark.getStringRep()
                            + "]"
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
     * The budget (estimation) for a merge task is the disk space (still) required for it to complete. As the merge progresses,
     * its budget decreases (as the bytes already written have been incorporated into the filesystem stats about the used disk space).
     */
    private final MergeTaskPriorityBlockingQueue queuedMergeTasks = new MergeTaskPriorityBlockingQueue();
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
    private final AvailableDiskSpacePeriodicMonitor availableDiskSpacePeriodicMonitor;

    private final List<MergeEventListener> mergeEventListeners = new CopyOnWriteArrayList<>();

    public static @Nullable ThreadPoolMergeExecutorService maybeCreateThreadPoolMergeExecutorService(
        ThreadPool threadPool,
        ClusterSettings clusterSettings,
        NodeEnvironment nodeEnvironment
    ) {
        if (clusterSettings.get(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING)) {
            return new ThreadPoolMergeExecutorService(threadPool, clusterSettings, nodeEnvironment);
        } else {
            // register no-op setting update consumers so that setting validations work properly
            // (some validations are bypassed if there are no update consumers registered),
            // i.e. to reject watermark and max headroom updates if the thread pool merge scheduler is disabled
            clusterSettings.addSettingsUpdateConsumer(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING, (ignored) -> {});
            clusterSettings.addSettingsUpdateConsumer(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING, (ignored) -> {});
            clusterSettings.addSettingsUpdateConsumer(INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING, (ignored) -> {});
            return null;
        }
    }

    private ThreadPoolMergeExecutorService(ThreadPool threadPool, ClusterSettings clusterSettings, NodeEnvironment nodeEnvironment) {
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxConcurrentMerges = threadPool.info(ThreadPool.Names.MERGE).getMax();
        // the intent here is to throttle down whenever we submit a task and no other task is running
        this.concurrentMergesFloorLimitForThrottling = 2;
        this.concurrentMergesCeilLimitForThrottling = maxConcurrentMerges * 2;
        assert concurrentMergesFloorLimitForThrottling <= concurrentMergesCeilLimitForThrottling;
        this.availableDiskSpacePeriodicMonitor = startDiskSpaceMonitoring(
            threadPool,
            nodeEnvironment.dataPaths(),
            clusterSettings,
            (availableDiskSpaceByteSize) -> this.queuedMergeTasks.updateBudget(availableDiskSpaceByteSize.getBytes())
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
        assert mergeTask.isRunning() == false;
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
        return queuedMergeTasks.isQueueEmpty() && runningMergeTasks.isEmpty() && ioThrottledMergeTasksCount.get() == 0L;
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
                        // Will block if there are backlogged merges until they're enqueued again
                        // (for e.g. if the per-shard concurrent merges count limit is reached).
                        // Will also block if there is insufficient budget (i.e. estimated available disk space
                        // for the smallest merge task to run to completion)
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
                        MergeTask smallestMergeTask = smallestMergeTaskWithReleasableBudget.element();
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
                            // The merge task is backlogged by the merge scheduler, try to get the next smallest one.
                            // It's then the duty of the said merge scheduler to re-enqueue the backlogged merge task when
                            // itself decides that the merge task could be run. Note that it is possible that this merge
                            // task is re-enqueued and re-took before the budget hold-up here is released below.
                        }
                    } finally {
                        // releases any budget that is still being allocated for the merge task
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

    /**
     * Start monitoring the available disk space, and update the available budget for running merge tasks
     * Note: this doesn't work correctly for nodes with multiple data paths, as it only considers the data path with the MOST
     * available disk space. In this case, merges will NOT be blocked for shards on data paths with insufficient available
     * disk space, as long as a single data path has enough available disk space to run merges for any shards that it stores
     * (i.e. multiple data path is not really supported when blocking merges due to insufficient available disk space
     * (but nothing blows up either, if using multiple data paths))
     */
    static AvailableDiskSpacePeriodicMonitor startDiskSpaceMonitoring(
        ThreadPool threadPool,
        NodeEnvironment.DataPath[] dataPaths,
        ClusterSettings clusterSettings,
        Consumer<ByteSizeValue> updateConsumer
    ) {
        AvailableDiskSpacePeriodicMonitor availableDiskSpacePeriodicMonitor = new AvailableDiskSpacePeriodicMonitor(
            dataPaths,
            threadPool,
            clusterSettings.get(INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING),
            clusterSettings.get(INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING),
            clusterSettings.get(INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING),
            updateConsumer
        );
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING,
            availableDiskSpacePeriodicMonitor::setHighStageWatermark
        );
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_MERGE_DISK_HIGH_MAX_HEADROOM_SETTING,
            availableDiskSpacePeriodicMonitor::setHighStageMaxHeadroom
        );
        clusterSettings.addSettingsUpdateConsumer(
            INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING,
            availableDiskSpacePeriodicMonitor::setCheckInterval
        );
        return availableDiskSpacePeriodicMonitor;
    }

    static class AvailableDiskSpacePeriodicMonitor implements Closeable {
        private static final Logger LOGGER = LogManager.getLogger(AvailableDiskSpacePeriodicMonitor.class);
        private final NodeEnvironment.DataPath[] dataPaths;
        private final ThreadPool threadPool;
        private volatile RelativeByteSizeValue highStageWatermark;
        private volatile ByteSizeValue highStageMaxHeadroom;
        private volatile TimeValue checkInterval;
        private final Consumer<ByteSizeValue> updateConsumer;
        private volatile boolean closed;
        private volatile Scheduler.Cancellable monitor;

        AvailableDiskSpacePeriodicMonitor(
            NodeEnvironment.DataPath[] dataPaths,
            ThreadPool threadPool,
            RelativeByteSizeValue highStageWatermark,
            ByteSizeValue highStageMaxHeadroom,
            TimeValue checkInterval,
            Consumer<ByteSizeValue> updateConsumer
        ) {
            this.dataPaths = dataPaths;
            this.threadPool = threadPool;
            this.highStageWatermark = highStageWatermark;
            this.highStageMaxHeadroom = highStageMaxHeadroom;
            this.checkInterval = checkInterval;
            this.updateConsumer = updateConsumer;
            this.closed = false;
            reschedule();
        }

        void setCheckInterval(TimeValue checkInterval) {
            this.checkInterval = checkInterval;
            reschedule();
        }

        void setHighStageWatermark(RelativeByteSizeValue highStageWatermark) {
            this.highStageWatermark = highStageWatermark;
        }

        void setHighStageMaxHeadroom(ByteSizeValue highStageMaxHeadroom) {
            this.highStageMaxHeadroom = highStageMaxHeadroom;
        }

        private synchronized void reschedule() {
            if (monitor != null) {
                monitor.cancel();
            }
            if (closed == false && checkInterval.duration() > 0) {
                monitor = threadPool.scheduleWithFixedDelay(this::run, checkInterval, threadPool.generic());
            }
        }

        @Override
        public void close() throws IOException {
            closed = true;
            reschedule();
        }

        private void run() {
            if (closed) {
                return;
            }
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
            // clamp available space to 0
            long maxMergeSizeLimit = Math.max(0L, mostAvailableDiskSpaceBytes);
            updateConsumer.accept(ByteSizeValue.ofBytes(maxMergeSizeLimit));
        }

        private static ByteSizeValue getFreeBytesThreshold(
            ByteSizeValue total,
            RelativeByteSizeValue watermark,
            ByteSizeValue maxHeadroom
        ) {
            // If bytes are given, they can be readily returned as free bytes.
            // If percentages are given, we need to calculate the free bytes.
            if (watermark.isAbsolute()) {
                return watermark.getAbsolute();
            }
            return ByteSizeValue.subtract(total, watermark.calculateValue(total, maxHeadroom));
        }
    }

    static class MergeTaskPriorityBlockingQueue extends PriorityBlockingQueueWithBudget<MergeTask> {
        MergeTaskPriorityBlockingQueue() {
            // start with unlimited budget (so this will behave like a regular priority queue until {@link #updateBudget} is invoked)
            // use the "remaining" merge size as the budget function so that the "budget" of taken elements is updated according
            // to the remaining disk space requirements of currently running merge tasks
            super(MergeTask::estimatedRemainingMergeSize, Long.MAX_VALUE);
        }

        // exposed for tests
        long getAvailableBudget() {
            return super.availableBudget;
        }

        // exposed for tests
        MergeTask peekQueue() {
            return enqueuedByBudget.peek();
        }
    }

    /**
     * Similar to a regular priority queue, but the {@link #take()} operation will also block if the smallest element
     * (according to the specified "budget" function) is larger than an updatable limit budget.
     */
    static class PriorityBlockingQueueWithBudget<E> {
        private final ToLongFunction<? super E> budgetFunction;
        protected final PriorityQueue<E> enqueuedByBudget;
        private final IdentityHashMap<Wrap<E>, Long> unreleasedBudgetPerElement;
        private final ReentrantLock lock;
        private final Condition elementAvailable;
        protected long availableBudget;

        PriorityBlockingQueueWithBudget(ToLongFunction<? super E> budgetFunction, long initialAvailableBudget) {
            this.budgetFunction = budgetFunction;
            this.enqueuedByBudget = new PriorityQueue<>(64, Comparator.comparingLong(budgetFunction));
            this.unreleasedBudgetPerElement = new IdentityHashMap<>();
            this.lock = new ReentrantLock();
            this.elementAvailable = lock.newCondition();
            this.availableBudget = initialAvailableBudget;
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

        /**
         * Dequeues the smallest element (according to the specified "budget" function) if its budget is below the available limit.
         * This method invocation blocks if the queue is empty or the element's budget is above the available limit.
         */
        ElementWithReleasableBudget take() throws InterruptedException {
            final ReentrantLock lock = this.lock;
            lock.lockInterruptibly();
            try {
                E peek;
                long peekBudget;
                // blocks until the smallest budget element fits the currently available budget
                while ((peek = enqueuedByBudget.peek()) == null || (peekBudget = budgetFunction.applyAsLong(peek)) > availableBudget) {
                    elementAvailable.await();
                }
                // deducts and holds up that element's budget from the available budget
                return new ElementWithReleasableBudget(enqueuedByBudget.poll(), peekBudget);
            } finally {
                lock.unlock();
            }
        }

        /**
         * Updates the available budged given the passed-in argument, from which it deducts the budget hold up by taken elements
         * that are still in use. The budget of in-use elements is also updated (by re-applying the budget function).
         * The newly updated budget is used to potentially block {@link #take()} operations if the smallest-budget enqueued element
         * is over this newly computed available budget.
         */
        void updateBudget(long availableBudget) {
            final ReentrantLock lock = this.lock;
            lock.lock();
            try {
                this.availableBudget = availableBudget;
                // update the per-element budget (these are all the elements that are using any budget)
                unreleasedBudgetPerElement.replaceAll((e, v) -> budgetFunction.applyAsLong(e.element()));
                // available budget is decreased by the used per-element budget (for all dequeued elements that are still in use)
                this.availableBudget -= unreleasedBudgetPerElement.values().stream().reduce(0L, Long::sum);
                elementAvailable.signalAll();
            } finally {
                lock.unlock();
            }
        }

        boolean isQueueEmpty() {
            return enqueuedByBudget.isEmpty();
        }

        int queueSize() {
            return enqueuedByBudget.size();
        }

        class ElementWithReleasableBudget implements Releasable {
            private final Wrap<E> wrappedElement;

            private ElementWithReleasableBudget(E element, long budget) {
                // Wrap the element in a brand-new instance that's used as the key in the
                // {@link PriorityBlockingQueueWithBudget#unreleasedBudgetPerElement} identity map.
                // This allows the same exact "element" instance to hold budgets multiple times concurrently.
                // This way we allow to re-enqueue and re-take an element before a previous take completed and
                // released the budget.
                this.wrappedElement = new Wrap<>(element);
                assert PriorityBlockingQueueWithBudget.this.lock.isHeldByCurrentThread();
                // the taken element holds up some budget
                var prev = unreleasedBudgetPerElement.put(wrappedElement, budget);
                assert prev == null;
                availableBudget -= budget;
                assert availableBudget >= 0L;
            }

            /**
             * Must be invoked when the caller is done with the element that it took from the queue.
             */
            @Override
            public void close() {
                final ReentrantLock lock = PriorityBlockingQueueWithBudget.this.lock;
                lock.lock();
                try {
                    assert unreleasedBudgetPerElement.containsKey(wrappedElement);
                    // when the taken element is not used anymore, the budget it holds is released
                    availableBudget += unreleasedBudgetPerElement.remove(wrappedElement);
                    elementAvailable.signalAll();
                } finally {
                    lock.unlock();
                }
            }

            E element() {
                return wrappedElement.element();
            }
        }

        private record Wrap<E>(E element) {}
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
        return queuedMergeTasks.queueSize();
    }

    // exposed for tests
    MergeTaskPriorityBlockingQueue getMergeTasksQueue() {
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

    @Override
    public void close() throws IOException {
        availableDiskSpacePeriodicMonitor.close();
    }
}
