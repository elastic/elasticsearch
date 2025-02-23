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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimitedIndexOutput;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
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
    private final Logger logger;
    private final MergeTracking mergeTracking;
    private final ThreadPoolMergeExecutorService threadPoolMergeExecutorService;
    private final PriorityQueue<MergeTask> backloggedMergeTasks = new PriorityQueue<>();
    private final Map<MergePolicy.OneMerge, MergeTask> currentlyRunningMergeTasks = new HashMap<>();
    // set when incoming merges should be throttled (i.e. restrict the indexing rate)
    private final AtomicBoolean shouldThrottleIncomingMerges = new AtomicBoolean();
    // how many {@link MergeTask}s have kicked off (this is used to name them).
    private final AtomicLong submittedMergeTaskCount = new AtomicLong();
    private final AtomicLong doneMergeTaskCount = new AtomicLong();
    private final CountDownLatch closedWithNoCurrentlyRunningMerges = new CountDownLatch(1);
    private volatile boolean closed = false;

    public ThreadPoolMergeScheduler(
        ShardId shardId,
        IndexSettings indexSettings,
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService
    ) {
        this.shardId = shardId;
        this.config = indexSettings.getMergeSchedulerConfig();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergeTracking = new MergeTracking(
            logger,
            () -> this.config.isAutoThrottle() ? threadPoolMergeExecutorService.getTargetMBPerSec() : Double.POSITIVE_INFINITY
        );
        this.threadPoolMergeExecutorService = threadPoolMergeExecutorService;
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
     * A callback that's invoked when indexing should throttle down indexing in order to let merging to catch up.
     */
    protected void enableIndexingThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    /**
     * A callback that's invoked when indexing should un-throttle because merging caught up.
     * This is invoked sometime after {@link #enableIndexingThrottling(int, int, int)} was invoked in the first place.
     */
    protected void disableIndexingThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    /**
     * A callback for exceptions thrown while merging.
     */
    protected void handleMergeException(Throwable t) {
        throw new MergePolicy.MergeException(t);
    }

    private boolean submitNewMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        try {
            MergeTask mergeTask = newMergeTask(mergeSource, merge, mergeTrigger);
            return threadPoolMergeExecutorService.submitMergeTask(mergeTask);
        } finally {
            checkMergeTaskThrottling();
        }
    }

    private void checkMergeTaskThrottling() {
        long submittedMergesCount = submittedMergeTaskCount.get();
        long doneMergesCount = doneMergeTaskCount.get();
        int executingMergesCount = currentlyRunningMergeTasks.size();
        int configuredMaxMergeCount = config.getMaxMergeCount();
        // both currently running and enqueued merge tasks are considered "active" for throttling purposes
        int activeMerges = (int) (submittedMergesCount - doneMergesCount);
        // maybe enable merge task throttling
        if (activeMerges > configuredMaxMergeCount && shouldThrottleIncomingMerges.getAndSet(true) == false) {
            enableIndexingThrottling(executingMergesCount, activeMerges - executingMergesCount, configuredMaxMergeCount);
        } else if (activeMerges <= configuredMaxMergeCount && shouldThrottleIncomingMerges.getAndSet(false)) {
            // maybe disable merge task throttling
            disableIndexingThrottling(executingMergesCount, activeMerges - executingMergesCount, configuredMaxMergeCount);
        }
    }

    private MergeTask newMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        // forced merges, as well as merges triggered when closing a shard, always run un-IO-throttled
        boolean isAutoThrottle = mergeTrigger != MergeTrigger.CLOSING && merge.getStoreMergeInfo().mergeMaxNumSegments() == -1;
        // IO throttling cannot be toggled for existing merge tasks, only new merge tasks pick up the updated IO throttling setting
        return new MergeTask(
            mergeSource,
            merge,
            isAutoThrottle && config.isAutoThrottle(),
            "Lucene Merge Task #" + submittedMergeTaskCount.incrementAndGet() + " for shard " + shardId
        );
    }

    // synchronized so that {@code #closed}, {@code #currentlyRunningMergeTasks} and {@code #backloggedMergeTasks} are modified atomically
    private synchronized boolean runNowOrBacklog(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        if (closed) {
            // Do not backlog or execute tasks when closing the merge scheduler, instead abort them.
            mergeTask.abortOnGoingMerge();
            throw new ElasticsearchException("merge task aborted because scheduler is shutting down");
        }
        if (currentlyRunningMergeTasks.size() < config.getMaxThreadCount()) {
            boolean added = currentlyRunningMergeTasks.put(mergeTask.onGoingMerge.getMerge(), mergeTask) == null;
            assert added : "starting merge task [" + mergeTask + "] registered as already running";
            return true;
        } else {
            backloggedMergeTasks.add(mergeTask);
            return false;
        }
    }

    private void mergeDone(MergeTask mergeTask) {
        synchronized (this) {
            boolean removed = currentlyRunningMergeTasks.remove(mergeTask.onGoingMerge.getMerge()) != null;
            assert removed : "completed merge task [" + mergeTask + "] not registered as running";
            // when one merge is done, maybe a backlogged one can now execute
            enqueueBackloggedTasks();
            // signal here, because, when closing, we wait for all currently running merges to finish
            maybeSignalAllMergesDoneAfterClose();
        }
        doneMergeTaskCount.incrementAndGet();
        checkMergeTaskThrottling();
    }

    private synchronized void maybeSignalAllMergesDoneAfterClose() {
        if (closed && currentlyRunningMergeTasks.isEmpty() && closedWithNoCurrentlyRunningMerges.getCount() > 0) {
            closedWithNoCurrentlyRunningMerges.countDown();
        }
    }

    private synchronized void enqueueBackloggedTasks() {
        int maxBackloggedTasksToEnqueue = config.getMaxThreadCount() - currentlyRunningMergeTasks.size();
        // enqueue all backlogged tasks when closing, as the queue expects all backlogged tasks to always be enqueued back
        while (closed || maxBackloggedTasksToEnqueue-- > 0) {
            MergeTask backloggedMergeTask = backloggedMergeTasks.poll();
            if (backloggedMergeTask == null) {
                break;
            }
            // no need to abort merge tasks now, they will be aborted on the spot when the scheduler gets to run them
            threadPoolMergeExecutorService.enqueueMergeTask(backloggedMergeTask);
        }
    }

    /**
     * Does the actual merge, by calling {@link org.apache.lucene.index.MergeScheduler.MergeSource#merge}
     */
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        mergeSource.merge(merge);
    }

    @Override
    public Directory wrapForMerge(MergePolicy.OneMerge merge, Directory in) {
        // Return a wrapped Directory which has rate-limited output.
        // Note: the rate limiter is only per thread (per merge). So, if there are multiple merge threads running
        // the combined IO rate per node is, roughly, 'thread_pool_size * merge_queue#targetMBPerSec', as
        // the per-thread IO rate is updated, best effort, for all running merge threads concomitantly.
        MergeTask mergeTask = currentlyRunningMergeTasks.get(merge);
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

    final class MergeTask implements Runnable, Comparable<MergeTask> {
        private final String name;
        private final AtomicLong mergeStartTimeNS;
        private final MergeSource mergeSource;
        private final OnGoingMerge onGoingMerge;
        private final MergeRateLimiter rateLimiter;
        private final boolean supportsIOThrottling;

        MergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, boolean supportsIOThrottling, String name) {
            this.name = name;
            this.mergeStartTimeNS = new AtomicLong();
            this.mergeSource = mergeSource;
            this.onGoingMerge = new OnGoingMerge(merge);
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
            this.supportsIOThrottling = supportsIOThrottling;
        }

        boolean runNowOrBacklog() {
            return ThreadPoolMergeScheduler.this.runNowOrBacklog(this);
        }

        @Override
        public int compareTo(MergeTask other) {
            // sort smaller merges first, so they are executed before larger ones
            return Long.compare(onGoingMerge.getMerge().estimatedMergeBytes, other.onGoingMerge.getMerge().estimatedMergeBytes);
        }

        public boolean supportsIOThrottling() {
            return supportsIOThrottling;
        }

        public void setIORateLimit(double mbPerSec) {
            if (supportsIOThrottling == false) {
                throw new IllegalArgumentException("merge task cannot be IO throttled");
            }
            this.rateLimiter.setMBPerSec(mbPerSec);
        }

        public boolean isRunning() {
            return mergeStartTimeNS.get() > 0L;
        }

        @Override
        public void run() {
            assert isRunning() == false;
            assert ThreadPoolMergeScheduler.this.currentlyRunningMergeTasks.containsKey(onGoingMerge.getMerge())
                : "runNowOrBacklog must be invoked before actually running the merge task";
            try {
                beforeMerge(onGoingMerge);
                try {
                    if (mergeStartTimeNS.compareAndSet(0L, System.nanoTime()) == false) {
                        throw new IllegalStateException("The merge task is already started");
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
                    } catch (Throwable t) {
                        if (t instanceof MergePolicy.MergeAbortedException) {
                            // OK to ignore. This is what Lucene's ConcurrentMergeScheduler does
                        } else {
                            handleMergeException(t);
                        }
                    } finally {
                        long tookMS = TimeValue.nsecToMSec(System.nanoTime() - mergeStartTimeNS.get());
                        mergeTracking.mergeFinished(onGoingMerge.getMerge(), onGoingMerge, tookMS);
                    }
                } finally {
                    if (verbose()) {
                        message(String.format(Locale.ROOT, "merge task %s end", this));
                    }
                    afterMerge(onGoingMerge);
                }
            } finally {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", this));
                }
                mergeDone(this);
                // kick-off next merge, if any
                merge(mergeSource, MergeTrigger.MERGE_FINISHED);
            }
        }

        void abortOnGoingMerge() {
            // This would interrupt an IndexWriter if it were actually performing the merge. We just set this here because it seems
            // appropriate as we are not going to move forward with the merge.
            onGoingMerge.getMerge().setAborted();
            // It is fine to mark this merge as finished. Lucene will eventually produce a new merge including this segment even if
            // this merge did not actually execute.
            mergeSource.onMergeFinished(onGoingMerge.getMerge());
            doneMergeTaskCount.incrementAndGet();
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
        closed = true;
        // enqueue any backlogged merge tasks, because the merge queue assumes that the backlogged tasks are always re-enqueued
        enqueueBackloggedTasks();
        maybeSignalAllMergesDoneAfterClose();
        try {
            closedWithNoCurrentlyRunningMerges.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            // this closes an executor that may be used by ongoing merges, so better close it only after all running merges finished
            super.close();
        }
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
}
