/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeRateLimiter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolMergeScheduler extends MergeScheduler implements ElasticsearchMergeScheduler {
    public static final Setting<Boolean> USE_THREAD_POOL_MERGE_SCHEDULER_SETTING = Setting.boolSetting(
        "indices.merge.scheduler.use_thread_pool",
        true,
        Setting.Property.NodeScope
    );
    private final ShardId shardId;
    private final MergeSchedulerConfig config;
    protected final Logger logger;
    private final MergeTracking mergeTracking;
    private final ThreadPoolMergeExecutorService threadPoolMergeExecutorService;
    private final PriorityQueue<MergeTask> backloggedMergeTasks = new PriorityQueue<>(
        16,
        Comparator.comparingLong(MergeTask::estimatedRemainingMergeSize)
    );
    private final Map<MergePolicy.OneMerge, MergeTask> runningMergeTasks = new HashMap<>();
    // set when incoming merges should be throttled (i.e. restrict the indexing rate)
    private final AtomicBoolean shouldThrottleIncomingMerges = new AtomicBoolean();
    // how many {@link MergeTask}s have kicked off (this is used to name them).
    private final AtomicLong submittedMergeTaskCount = new AtomicLong();
    private final AtomicLong doneMergeTaskCount = new AtomicLong();
    private final CountDownLatch closedWithNoRunningMerges = new CountDownLatch(1);
    private volatile boolean closed = false;
    private final MergeMemoryEstimateProvider mergeMemoryEstimateProvider;

    /**
     * Creates a thread-pool-based merge scheduler that runs merges in a thread pool.
     *
     * @param shardId                        the shard id associated with this merge scheduler
     * @param indexSettings                  used to obtain the {@link MergeSchedulerConfig}
     * @param threadPoolMergeExecutorService the executor service used to execute merge tasks from this scheduler
     * @param mergeMemoryEstimateProvider    provides an estimate for how much memory a merge will take
     */
    public ThreadPoolMergeScheduler(
        ShardId shardId,
        IndexSettings indexSettings,
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService,
        MergeMemoryEstimateProvider mergeMemoryEstimateProvider
    ) {
        this.shardId = shardId;
        this.config = indexSettings.getMergeSchedulerConfig();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergeTracking = new MergeTracking(
            logger,
            () -> this.config.isAutoThrottle()
                ? ByteSizeValue.ofBytes(threadPoolMergeExecutorService.getTargetIORateBytesPerSec()).getMbFrac()
                : Double.POSITIVE_INFINITY
        );
        this.threadPoolMergeExecutorService = threadPoolMergeExecutorService;
        this.mergeMemoryEstimateProvider = mergeMemoryEstimateProvider;
    }

    @Override
    public Set<OnGoingMerge> onGoingMerges() {
        return mergeTracking.onGoingMerges();
    }

    @Override
    public MergeStats stats() {
        return mergeTracking.stats();
    }

    @Override
    public MergeScheduler getMergeScheduler() {
        return this;
    }

    @Override
    public void refreshConfig() {
        // if maxMergeCount changed, maybe we need to toggle merge task throttling
        checkMergeTaskThrottling();
        // if maxThreadCount changed, maybe some backlogged merges are now allowed to run
        enqueueBackloggedTasks();
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) {
        if (closed) {
            // avoid pulling from the merge source when closing
            return;
        }
        MergePolicy.OneMerge merge = null;
        try {
            merge = mergeSource.getNextMerge();
        } catch (IllegalStateException e) {
            if (verbose()) {
                message("merge task poll failed, likely that index writer is failed");
            }
            // ignore exception, we expect the IW failure to be logged elsewhere
        }
        if (merge != null) {
            submitNewMergeTask(mergeSource, merge, trigger);
        }
    }

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void beforeMerge(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic after an actual merge starts.
     */
    protected void afterMerge(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic when a merge is queued.
     */
    protected void mergeQueued(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic after a merge is executed or aborted.
     */
    protected void mergeExecutedOrAborted(OnGoingMerge merge) {}

    /**
     * A callback that's invoked when indexing should throttle down indexing in order to let merging to catch up.
     */
    protected void enableIndexingThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    /**
     * A callback that's invoked when indexing should un-throttle because merging caught up.
     * This is invoked sometime after {@link #enableIndexingThrottling(int, int, int)} was invoked in the first place.
     */
    protected void disableIndexingThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    /**
     * Returns true if scheduled merges should be skipped (aborted)
     */
    protected boolean shouldSkipMerge() {
        return false;
    }

    /**
     * Returns true if IO-throttling is enabled
     */
    protected boolean isAutoThrottle() {
        return config.isAutoThrottle();
    }

    /**
     * Returns the maximum number of active merges before being throttled
     */
    protected int getMaxMergeCount() {
        return config.getMaxMergeCount();
    }

    /**
     * Returns the maximum number of threads running merges before being throttled
     */
    protected int getMaxThreadCount() {
        return config.getMaxThreadCount();
    }

    /**
     * A callback for exceptions thrown while merging.
     */
    protected void handleMergeException(Throwable t) {
        throw new MergePolicy.MergeException(t);
    }

    // package-private for tests
    boolean submitNewMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        try {
            MergeTask mergeTask = newMergeTask(mergeSource, merge, mergeTrigger);
            mergeQueued(mergeTask.onGoingMerge);
            return threadPoolMergeExecutorService.submitMergeTask(mergeTask);
        } finally {
            checkMergeTaskThrottling();
        }
    }

    // package-private for tests
    MergeTask newMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        // forced merges, as well as merges triggered when closing a shard, always run un-IO-throttled
        boolean isAutoThrottle = mergeTrigger != MergeTrigger.CLOSING && merge.getStoreMergeInfo().mergeMaxNumSegments() == -1;
        // IO throttling cannot be toggled for existing merge tasks, only new merge tasks pick up the updated IO throttling setting
        long estimateMergeMemoryBytes = mergeMemoryEstimateProvider.estimateMergeMemoryBytes(merge);
        return new MergeTask(
            mergeSource,
            merge,
            isAutoThrottle && isAutoThrottle(),
            "Lucene Merge Task #" + submittedMergeTaskCount.incrementAndGet() + " for shard " + shardId,
            estimateMergeMemoryBytes
        );
    }

    private void checkMergeTaskThrottling() {
        long submittedMergesCount = submittedMergeTaskCount.get();
        long doneMergesCount = doneMergeTaskCount.get();
        int runningMergesCount = runningMergeTasks.size();
        int configuredMaxMergeCount = getMaxMergeCount();
        // both currently running and enqueued merge tasks are considered "active" for throttling purposes
        int activeMerges = (int) (submittedMergesCount - doneMergesCount);
        if (activeMerges > configuredMaxMergeCount
            // only throttle indexing if disk IO is un-throttled, and we still can't keep up with the merge load
            && threadPoolMergeExecutorService.usingMaxTargetIORateBytesPerSec()
            && shouldThrottleIncomingMerges.get() == false) {
            // maybe enable merge task throttling
            synchronized (shouldThrottleIncomingMerges) {
                if (shouldThrottleIncomingMerges.getAndSet(true) == false) {
                    enableIndexingThrottling(runningMergesCount, activeMerges - runningMergesCount, configuredMaxMergeCount);
                }
            }
        } else if (activeMerges <= configuredMaxMergeCount && shouldThrottleIncomingMerges.get()) {
            // maybe disable merge task throttling
            synchronized (shouldThrottleIncomingMerges) {
                if (shouldThrottleIncomingMerges.getAndSet(false)) {
                    disableIndexingThrottling(runningMergesCount, activeMerges - runningMergesCount, configuredMaxMergeCount);
                }
            }
        }
    }

    // exposed for tests
    // synchronized so that {@code #closed}, {@code #runningMergeTasks} and {@code #backloggedMergeTasks} are modified atomically
    synchronized Schedule schedule(MergeTask mergeTask) {
        assert mergeTask.hasStartedRunning() == false;
        if (closed) {
            // do not run or backlog tasks when closing the merge scheduler, instead abort them
            return Schedule.ABORT;
        } else if (shouldSkipMerge()) {
            if (verbose()) {
                message(String.format(Locale.ROOT, "skipping merge task %s", mergeTask));
            }
            return Schedule.ABORT;
        } else if (runningMergeTasks.size() < getMaxThreadCount()) {
            boolean added = runningMergeTasks.put(mergeTask.onGoingMerge.getMerge(), mergeTask) == null;
            assert added : "starting merge task [" + mergeTask + "] registered as already running";
            return Schedule.RUN;
        } else {
            assert mergeTask.hasStartedRunning() == false;
            backloggedMergeTasks.add(mergeTask);
            return Schedule.BACKLOG;
        }
    }

    // exposed for tests
    synchronized void mergeTaskFinishedRunning(MergeTask mergeTask) {
        boolean removed = runningMergeTasks.remove(mergeTask.onGoingMerge.getMerge()) != null;
        assert removed : "completed merge task [" + mergeTask + "] not registered as running";
        // when one merge is done, maybe a backlogged one can now execute
        enqueueBackloggedTasks();
        // signal here, because, when closing, we wait for all currently running merges to finish
        maybeSignalAllMergesDoneAfterClose();
    }

    private void mergeTaskDone(OnGoingMerge merge) {
        doneMergeTaskCount.incrementAndGet();
        mergeExecutedOrAborted(merge);
        checkMergeTaskThrottling();
    }

    private synchronized void maybeSignalAllMergesDoneAfterClose() {
        if (closed && runningMergeTasks.isEmpty()) {
            closedWithNoRunningMerges.countDown();
        }
    }

    private synchronized void enqueueBackloggedTasks() {
        int maxBackloggedTasksToEnqueue = getMaxThreadCount() - runningMergeTasks.size();
        // enqueue all backlogged tasks when closing, as the queue expects all backlogged tasks to always be enqueued back
        while (closed || maxBackloggedTasksToEnqueue-- > 0) {
            MergeTask backloggedMergeTask = backloggedMergeTasks.poll();
            if (backloggedMergeTask == null) {
                break;
            }
            // no need to abort merge tasks now, they will be aborted on the spot when the scheduler gets to run them
            threadPoolMergeExecutorService.reEnqueueBackloggedMergeTask(backloggedMergeTask);
        }
    }

    /**
     * Does the actual merge, by calling {@link org.apache.lucene.index.MergeScheduler.MergeSource#merge}
     */
    void doMerge(MergeSource mergeSource, MergePolicy.OneMerge oneMerge) {
        try {
            mergeSource.merge(oneMerge);
        } catch (Throwable t) {
            // OK to ignore MergeAbortedException. This is what Lucene's ConcurrentMergeScheduler does.
            if (t instanceof MergePolicy.MergeAbortedException == false) {
                handleMergeException(t);
            }
        }
    }

    @Override
    public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
        // Return a wrapped Directory which has rate-limited output.
        // Note: the rate limiter is only per thread (per merge). So, if there are multiple merge threads running
        // the combined IO rate per node is, roughly, 'thread_pool_size * merge_queue#targetMBPerSec', as
        // the per-thread IO rate is updated, best effort, for all running merge threads concomitantly.
        if (merge.isAborted()) {
            // merges can theoretically be aborted at any moment
            return in;
        }
        MergeTask mergeTask = runningMergeTasks.get(merge);
        if (mergeTask == null) {
            throw new IllegalStateException("associated merge task for executing merge not found");
        }
        return new FilterDirectory(in) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                ensureOpen();

                // This Directory is only supposed to be used during merging,
                // so all writes should have MERGE context, else there is a bug
                // somewhere that is failing to pass down the right IOContext:
                assert context.context() == IOContext.Context.MERGE : "got context=" + context.context();

                return new RateLimitedIndexOutput(mergeTask.rateLimiter, in.createOutput(name, context));
            }
        };
    }

    class MergeTask implements Runnable {
        private final String name;
        private final AtomicLong mergeStartTimeNS;
        private final MergeSource mergeSource;
        private final OnGoingMerge onGoingMerge;
        private final MergeRateLimiter rateLimiter;
        private final boolean supportsIOThrottling;
        private final long mergeMemoryEstimateBytes;

        MergeTask(
            MergeSource mergeSource,
            MergePolicy.OneMerge merge,
            boolean supportsIOThrottling,
            String name,
            long mergeMemoryEstimateBytes
        ) {
            this.name = name;
            this.mergeStartTimeNS = new AtomicLong();
            this.mergeSource = mergeSource;
            this.onGoingMerge = new OnGoingMerge(merge);
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
            this.supportsIOThrottling = supportsIOThrottling;
            this.mergeMemoryEstimateBytes = mergeMemoryEstimateBytes;
        }

        Schedule schedule() {
            return ThreadPoolMergeScheduler.this.schedule(this);
        }

        public boolean supportsIOThrottling() {
            return supportsIOThrottling;
        }

        public void setIORateLimit(long ioRateLimitBytesPerSec) {
            if (supportsIOThrottling == false) {
                throw new IllegalArgumentException("merge task cannot be IO throttled");
            }
            this.rateLimiter.setMBPerSec(ByteSizeValue.ofBytes(ioRateLimitBytesPerSec).getMbFrac());
        }

        /**
         * Returns {@code true} if this task is currently running, or was run in the past.
         * An aborted task (see {@link #abort()}) is considered as NOT run.
         */
        public boolean hasStartedRunning() {
            boolean isRunning = mergeStartTimeNS.get() > 0L;
            assert isRunning != false || rateLimiter.getTotalBytesWritten() == 0L;
            return isRunning;
        }

        /**
         * Runs the merge associated to this task. MUST be invoked after {@link #schedule()} returned {@link Schedule#RUN},
         * to confirm that the associated {@link MergeScheduler} assents to run the merge.
         * Either one of {@link #run()} or {@link #abort()} MUST be invoked exactly once for evey {@link MergeTask}.
         * After the merge is finished, this will also submit any follow-up merges from the task's merge source.
         */
        @Override
        public void run() {
            assert hasStartedRunning() == false;
            assert ThreadPoolMergeScheduler.this.runningMergeTasks.containsKey(onGoingMerge.getMerge())
                : "runNowOrBacklog must be invoked before actually running the merge task";
            try {
                beforeMerge(onGoingMerge);
                try {
                    if (mergeStartTimeNS.compareAndSet(0L, System.nanoTime()) == false) {
                        throw new IllegalStateException("The merge task is already started or aborted");
                    }
                    mergeTracking.mergeStarted(onGoingMerge);
                    if (verbose()) {
                        message(String.format(Locale.ROOT, "merge task %s start", this));
                    }
                    try {
                        doMerge(mergeSource, onGoingMerge.getMerge());
                        if (verbose()) {
                            message(
                                String.format(
                                    Locale.ROOT,
                                    "merge task %s merge segment [%s] done estSize=%.1f MB (written=%.1f MB) "
                                        + "runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s",
                                    this,
                                    getSegmentName(onGoingMerge.getMerge()),
                                    bytesToMB(onGoingMerge.getMerge().estimatedMergeBytes),
                                    bytesToMB(rateLimiter.getTotalBytesWritten()),
                                    nsToSec(System.nanoTime() - mergeStartTimeNS.get()),
                                    nsToSec(rateLimiter.getTotalStoppedNS()),
                                    nsToSec(rateLimiter.getTotalPausedNS()),
                                    rateToString(rateLimiter.getMBPerSec())
                                )
                            );
                        }
                    } finally {
                        long tookMS = TimeValue.nsecToMSec(System.nanoTime() - mergeStartTimeNS.get());
                        mergeTracking.mergeFinished(onGoingMerge.getMerge(), onGoingMerge, tookMS);
                    }
                } finally {
                    afterMerge(onGoingMerge);
                }
            } finally {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", this));
                }
                try {
                    mergeTaskFinishedRunning(this);
                } finally {
                    mergeTaskDone(onGoingMerge);
                }
                try {
                    // kick-off any follow-up merge
                    merge(mergeSource, MergeTrigger.MERGE_FINISHED);
                } catch (@SuppressWarnings("unused") AlreadyClosedException ace) {
                    // OK, this is what the {@code ConcurrentMergeScheduler} does
                }
            }
        }

        /**
         * Aborts the merge task, for e.g. when the {@link MergeScheduler}, or the
         * {@link ThreadPoolMergeExecutorService} are closing. Either one of {@link #run()} or {@link #abort()}
         * MUST be invoked exactly once for evey {@link MergeTask}.
         * An aborted merge means that the segments involved will be made available
         * (by the {@link org.apache.lucene.index.IndexWriter}) to any subsequent merges.
         */
        void abort() {
            assert hasStartedRunning() == false;
            assert ThreadPoolMergeScheduler.this.runningMergeTasks.containsKey(onGoingMerge.getMerge()) == false
                : "cannot abort a merge task that's already running";
            if (verbose()) {
                message(String.format(Locale.ROOT, "merge task %s aborted", this));
            }
            // {@code IndexWriter} checks the abort flag internally, while running the merge.
            // The segments of an aborted merge become available to subsequent merges.
            onGoingMerge.getMerge().setAborted();
            try {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s start abort", this));
                }
                // mark the merge task as running, even though the merge itself is aborted and the task will run for a brief time only
                if (mergeStartTimeNS.compareAndSet(0L, System.nanoTime()) == false) {
                    throw new IllegalStateException("The merge task is already started or aborted");
                }
                // This ensures {@code OneMerge#close} gets invoked.
                // {@code IndexWriter} considers a merge as "running" once it has been pulled from the {@code MergeSource#getNextMerge},
                // so in theory it's not enough to just call {@code MergeSource#onMergeFinished} on it (as for "pending" ones).
                doMerge(mergeSource, onGoingMerge.getMerge());
            } finally {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end abort", this));
                }
                mergeTaskDone(onGoingMerge);
            }
        }

        /**
         * Before the merge task started running, this returns the estimated required disk space for the merge to complete
         * (i.e. the estimated disk space size of the resulting segment following the merge).
         * While the merge is running, the returned estimation is updated to take into account the data that's already been written.
         * After the merge completes, the estimation returned here should ideally be close to "0".
         */
        long estimatedRemainingMergeSize() {
            // TODO is it possible that `estimatedMergeBytes` be `0` for correctly initialize merges,
            // or is it always the case that if `estimatedMergeBytes` is `0` that means that the merge has not yet been initialized?
            long estimatedMergeSize = onGoingMerge.getMerge().getStoreMergeInfo().estimatedMergeBytes();
            return Math.max(0L, estimatedMergeSize - rateLimiter.getTotalBytesWritten());
        }

        public long getMergeMemoryEstimateBytes() {
            return mergeMemoryEstimateBytes;
        }

        public OnGoingMerge getOnGoingMerge() {
            return onGoingMerge;
        }

        @Override
        public String toString() {
            return name + (onGoingMerge.getMerge().isAborted() ? " (aborted)" : "");
        }
    }

    @Override
    /* Overridden to route messages to our logger too, in addition to the {@link org.apache.lucene.util.InfoStream} that lucene uses. */
    protected boolean verbose() {
        if (logger.isTraceEnabled()) {
            return true;
        }
        return super.verbose();
    }

    @Override
    /* Overridden to route messages to our logger too, in addition to the {@link org.apache.lucene.util.InfoStream} that lucene uses. */
    protected void message(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", message);
        }
        super.message(message);
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            closed = true;
            // enqueue any backlogged merge tasks, because the merge queue assumes that the backlogged tasks are always re-enqueued
            enqueueBackloggedTasks();
            // signal if there aren't any currently running merges
            maybeSignalAllMergesDoneAfterClose();
        }
        try {
            closedWithNoRunningMerges.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // this closes an executor that may be used by ongoing merges, so better close it only after all running merges finished
            super.close();
        }
    }

    // exposed for tests
    PriorityQueue<MergeTask> getBackloggedMergeTasks() {
        return backloggedMergeTasks;
    }

    // exposed for tests
    Map<MergePolicy.OneMerge, MergeTask> getRunningMergeTasks() {
        return runningMergeTasks;
    }

    private static double nsToSec(long ns) {
        return ns / (double) TimeUnit.SECONDS.toNanos(1);
    }

    private static double bytesToMB(long bytes) {
        return bytes / 1024. / 1024.;
    }

    private static String getSegmentName(MergePolicy.OneMerge merge) {
        return merge.getMergeInfo() != null ? merge.getMergeInfo().info.name : "_na_";
    }

    private static String rateToString(double mbPerSec) {
        if (mbPerSec == 0.0) {
            return "stopped";
        } else if (mbPerSec == Double.POSITIVE_INFINITY) {
            return "unlimited";
        } else {
            return String.format(Locale.ROOT, "%.1f MB/sec", mbPerSec);
        }
    }

    enum Schedule {
        ABORT,
        RUN,
        BACKLOG
    }
}
