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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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
    private final ThreadPoolMergeQueue threadPoolMergeQueue;
    private final PriorityQueue<MergeTask> backloggedMergeTasks = new PriorityQueue<>();
    private final Map<MergePolicy.OneMerge, MergeTask> currentlyRunningMergeTasks = new HashMap<>();
    // set when incoming merges should be throttled (i.e. restrict the indexing rate)
    private final AtomicBoolean shouldThrottleIncomingMerges = new AtomicBoolean();
    // how many {@link MergeTask}s have kicked off (this is used to name them).
    private final AtomicLong mergeTaskCount = new AtomicLong();
    private final AtomicLong mergeTaskDoneCount = new AtomicLong();

    public ThreadPoolMergeScheduler(ShardId shardId, IndexSettings indexSettings, ThreadPoolMergeQueue threadPoolMergeQueue) {
        this.shardId = shardId;
        this.config = indexSettings.getMergeSchedulerConfig();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergeTracking = new MergeTracking(
            logger,
            () -> this.config.isAutoThrottle() ? threadPoolMergeQueue.getTargetMBPerSec() : Double.POSITIVE_INFINITY
        );
        this.threadPoolMergeQueue = threadPoolMergeQueue;
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
        enqueueBacklogged();
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        MergePolicy.OneMerge merge = mergeSource.getNextMerge();
        if (merge != null) {
            submitNewMergeTask(mergeSource, merge, trigger);
        }
    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void beforeMerge(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic after an actual merge starts.
     */
    protected void afterMerge(OnGoingMerge merge) {}

    protected void enableMergeTaskThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    protected void disableMergeTaskThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }

    protected void handleMergeException(Throwable t) {
        throw new MergePolicy.MergeException(t);
    }

    private void submitNewMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        MergeTask mergeTask = newMergeTask(mergeSource, merge, mergeTrigger);
        assert mergeTask.isRunning() == false;
        threadPoolMergeQueue.submitMergeTask(mergeTask);
        checkMergeTaskThrottling();
    }

    private void checkMergeTaskThrottling() {
        long submittedMergesCount = mergeTaskCount.get();
        long doneMergesCount = mergeTaskDoneCount.get();
        int executingMergesCount = currentlyRunningMergeTasks.size();
        int configuredMaxMergeCount = config.getMaxMergeCount();
        // both currently running and enqueued merge tasks are considered "active" for throttling purposes
        int activeMerges = (int) (submittedMergesCount - doneMergesCount);
        // maybe enable merge task throttling
        if (activeMerges > configuredMaxMergeCount && shouldThrottleIncomingMerges.getAndSet(true) == false) {
            enableMergeTaskThrottling(executingMergesCount, activeMerges - executingMergesCount, configuredMaxMergeCount);
        } else if (activeMerges <= configuredMaxMergeCount && shouldThrottleIncomingMerges.getAndSet(false)) {
            // maybe disable merge task throttling
            disableMergeTaskThrottling(executingMergesCount, activeMerges - executingMergesCount, configuredMaxMergeCount);
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
            "Lucene Merge Task #" + mergeTaskCount.incrementAndGet() + " for shard " + shardId
        );
    }

    // synchronized so that {@code #currentlyRunningMergeTasks} and {@code #backloggedMergeTasks} are modified atomically
    private synchronized boolean runNowOrBacklog(MergeTask mergeTask) {
        assert mergeTask.isRunning() == false;
        if (currentlyRunningMergeTasks.size() >= config.getMaxThreadCount()) {
            backloggedMergeTasks.add(mergeTask);
            return false;
        } else {
            boolean added = currentlyRunningMergeTasks.put(mergeTask.onGoingMerge.getMerge(), mergeTask) == null;
            assert added : "starting merge task [" + mergeTask + "] registered as already running";
            return true;
        }
    }

    private void mergeDone(MergeTask mergeTask) {
        assert mergeTask.isRunning();
        synchronized (this) {
            boolean removed = currentlyRunningMergeTasks.remove(mergeTask.onGoingMerge.getMerge()) != null;
            assert removed : "completed merge task [" + mergeTask + "] not registered as running";
            // when one merge is done, maybe a backlogged one can now execute
            enqueueBacklogged();
        }
        mergeTaskDoneCount.incrementAndGet();
        checkMergeTaskThrottling();
    }

    private synchronized void enqueueBacklogged() {
        int maxBackloggedTasksToEnqueue = config.getMaxThreadCount() - currentlyRunningMergeTasks.size();
        while (maxBackloggedTasksToEnqueue-- > 0) {
            MergeTask backloggedMergeTask = backloggedMergeTasks.poll();
            if (backloggedMergeTask == null) {
                break;
            }
            threadPoolMergeQueue.enqueueMergeTask(backloggedMergeTask);
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
        // Note: the rate limiter is only per thread. So, if there are multiple merge threads running
        // and throttling is required, each thread will be throttled independently.
        // The implication of this, is that the total IO rate could be higher than the target rate.
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

    final class MergeTask extends AbstractRunnable implements Comparable<MergeTask> {
        private final String name;
        private final SetOnce<Long> mergeStartTimeNS;
        private final MergeSource mergeSource;
        private final OnGoingMerge onGoingMerge;
        private final MergeRateLimiter rateLimiter;
        private final boolean supportsIOThrottling;

        MergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, boolean supportsIOThrottling, String name) {
            this.name = name;
            this.mergeStartTimeNS = new SetOnce<>();
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
            assert onGoingMerge.getMerge().estimatedMergeBytes > 0 : "estimated merge bytes not set";
            assert other.onGoingMerge.getMerge().estimatedMergeBytes > 0 : "estimated merge bytes not set";
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
            return mergeStartTimeNS.get() != null;
        }

        @Override
        public void doRun() throws Exception {
            if (isRunning()) {
                throw new IllegalStateException("Cannot run the same merge task multiple times");
            }
            mergeStartTimeNS.set(System.nanoTime());
            try {
                beforeMerge(onGoingMerge);
                mergeTracking.mergeStarted(onGoingMerge);
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s start", this));
                }
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
                } else if (t instanceof Exception == false) {
                    // onFailure and onAfter should better be called for Errors too
                    throw new ExceptionWrappingError(t);
                } else {
                    throw t;
                }
            }
        }

        @Override
        public void onAfter() {
            if (isRunning() == false) {
                throw new IllegalStateException("onAfter must only be invoked after doRun");
            }
            try {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", this));
                }
                afterMerge(onGoingMerge);
            } finally {
                long tookMS = TimeValue.nsecToMSec(System.nanoTime() - mergeStartTimeNS.get());
                try {
                    mergeTracking.mergeFinished(onGoingMerge.getMerge(), onGoingMerge, tookMS);
                } finally {
                    mergeDone(this);
                    // kick-off next merge, if any
                    MergePolicy.OneMerge nextMerge = null;
                    try {
                        nextMerge = mergeSource.getNextMerge();
                    } catch (IllegalStateException e) {
                        if (verbose()) {
                            message("merge task poll failed, likely that index writer is failed");
                        }
                        // ignore exception, we expect the IW failure to be logged elsewhere
                    }
                    if (nextMerge != null) {
                        submitNewMergeTask(mergeSource, nextMerge, MergeTrigger.MERGE_FINISHED);
                    }
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            if (isRunning() == false) {
                throw new IllegalStateException("onFailure must only be invoked after doRun");
            }
            assert this.mergeStartTimeNS.get() != null : "onFailure should always be invoked after doRun";
            // most commonly the merge should've already be aborted by now,
            // plus the engine is probably going to be failed when any merge fails,
            // but keep this in case something believes calling `MergeTask#onFailure` is a sane way to abort a merge
            abortOnGoingMerge();
            handleMergeException(ExceptionWrappingError.maybeUnwrapCause(e));
        }

        @Override
        public void onRejection(Exception e) {
            if (isRunning()) {
                throw new IllegalStateException("A running merge cannot be rejected for running");
            }
            if (verbose()) {
                message(String.format(Locale.ROOT, "merge task [%s] rejected by thread pool, aborting", onGoingMerge.getId()));
            }
            abortOnGoingMerge();
            mergeDone(this);
        }

        private void abortOnGoingMerge() {
            // This would interrupt an IndexWriter if it were actually performing the merge. We just set this here because it seems
            // appropriate as we are not going to move forward with the merge.
            onGoingMerge.getMerge().setAborted();
            // It is fine to mark this merge as finished. Lucene will eventually produce a new merge including this segment even if
            // this merge did not actually execute.
            mergeSource.onMergeFinished(onGoingMerge.getMerge());
        }

        @Override
        public String toString() {
            return name;
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

    private static class ExceptionWrappingError extends RuntimeException {
        private static Throwable maybeUnwrapCause(Exception e) {
            if (e instanceof ExceptionWrappingError exceptionWrappingError) {
                return exceptionWrappingError.getCause();
            }
            return e;
        }

        private ExceptionWrappingError(Throwable errorCause) {
            super(errorCause);
        }
    }
}
