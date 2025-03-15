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
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ThreadPoolMergeScheduler extends MergeScheduler implements ElasticsearchMergeScheduler {
    /**
     * Floor for IO write rate limit (we will never go any lower than this)
     */
    private static final double MIN_MERGE_MB_PER_SEC = 5.0;
    /**
     * Ceiling for IO write rate limit (we will never go any higher than this)
     */
    private static final double MAX_MERGE_MB_PER_SEC = 10240.0;
    /**
     * Initial value for IO write rate limit when doAutoIOThrottle is true
     */
    private static final double START_MB_PER_SEC = 20.0;
    /**
     * Current IO write throttle rate, for all merge, across all merge schedulers (shards) on the node
     */
    private static volatile double targetMBPerSec = START_MB_PER_SEC;
    /**
     * The set of all active merges, across all merge schedulers (i.e. across all shards), on the local node.
     * This is used to implement auto IO throttling that's the same across all merge schedulers.
     */
    private static final Set<MergeTask> activeThrottledMergeTasksAcrossSchedulersSet = new HashSet<>();

    private final MergeSchedulerConfig config;
    private final Logger logger;
    // per-scheduler merge stats
    private final MergeTracking mergeTracking;
    // this
    private final ExecutorService executorService;
    // the size of the per-node
    private final int maxThreadPoolSize;
    // used to communicate the IO rate limit to the {@IndexOutput} that's actually writing the merge
    private final ThreadLocal<MergeRateLimiter> onGoingMergeRateLimiter = new ThreadLocal<>();
    private final PriorityQueue<MergeTask> activeMergeTasksLocalSchedulerQueue = new PriorityQueue<>();
    private final List<MergeTask> activeMergeTasksExecutingOnLocalSchedulerList = new ArrayList<>();
    // set when incoming merges should be throttled
    private final AtomicBoolean isThrottling = new AtomicBoolean();
    // how many {@link MergeTask}s have kicked off (this is used to name them).
    private final AtomicLong mergeTaskCount = new AtomicLong();

    public ThreadPoolMergeScheduler(ShardId shardId, IndexSettings indexSettings, ThreadPool threadPool) {
        this.config = indexSettings.getMergeSchedulerConfig();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergeTracking = new MergeTracking(logger, () -> this.config.isAutoThrottle() ? targetMBPerSec : Double.POSITIVE_INFINITY);
        // all merge schedulers must use the same executor of the same thread pool
        this.executorService = threadPool.executor(ThreadPool.Names.MERGE);
        this.maxThreadPoolSize = threadPool.info(ThreadPool.Names.MERGE).getMax();
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
        // in case max merge count changed
        maybeActivateThrottling();
        maybeDeactivateThrottling();
        // in case max thread count changed (and more merges can be running simultaneously)
        while (maybeExecuteNextMerge()) {
        }
        // the IO auto-throttled setting change is only honoured for new merges
        // (existing ones continue with the value of the setting when the merge created (queued))
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

    protected void activateThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

    protected void deactivateThrottling(int numRunningMerges, int numQueuedMerges, int configuredMaxMergeCount) {}

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
        if (mergeTask.isAutoThrottle) {
            trackNewActiveThrottledMergeTask(mergeTask, maxThreadPoolSize);
        }
        synchronized (this) {
            activeMergeTasksLocalSchedulerQueue.add(mergeTask);
        }
        maybeExecuteNextMerge();
        maybeActivateThrottling();
    }

    private void mergeDone(MergeTask mergeTask) {
        synchronized (this) {
            activeMergeTasksExecutingOnLocalSchedulerList.remove(mergeTask);
        }
        maybeExecuteNextMerge();
        maybeDeactivateThrottling();
    }

    private boolean maybeExecuteNextMerge() {
        MergeTask mergeTask;
        synchronized (this) {
            if (activeMergeTasksExecutingOnLocalSchedulerList.size() >= config.getMaxThreadCount()) {
                // enough concurrent merges per scheduler (per shard) are already running
                return false;
            }
            mergeTask = activeMergeTasksLocalSchedulerQueue.poll();
            if (mergeTask == null) {
                // no more merges to execute
                return false;
            }
            activeMergeTasksExecutingOnLocalSchedulerList.add(mergeTask);
        }
        executorService.execute(mergeTask);
        return true;
    }

    private void maybeActivateThrottling() {
        int numRunningMerges = activeMergeTasksExecutingOnLocalSchedulerList.size();
        int numQueuedMerges = activeMergeTasksLocalSchedulerQueue.size();
        int configuredMaxMergeCount = config.getMaxMergeCount();
        // both currently running and enqueued count as "active" for throttling purposes
        if (numRunningMerges + numQueuedMerges > configuredMaxMergeCount && isThrottling.getAndSet(true) == false) {
            activateThrottling(numRunningMerges, numQueuedMerges, configuredMaxMergeCount);
        }
    }

    private void maybeDeactivateThrottling() {
        int numRunningMerges = activeMergeTasksExecutingOnLocalSchedulerList.size();
        int numQueuedMerges = activeMergeTasksLocalSchedulerQueue.size();
        int configuredMaxMergeCount = config.getMaxMergeCount();
        // both currently running and enqueued count as "active" for throttling purposes
        if (numRunningMerges + numQueuedMerges <= configuredMaxMergeCount && isThrottling.getAndSet(false)) {
            deactivateThrottling(numRunningMerges, numQueuedMerges, configuredMaxMergeCount);
        }
    }

    private static double maybeUpdateTargetMBPerSec(int poolSize) {
        if (activeThrottledMergeTasksAcrossSchedulersSet.size() < poolSize * 2 && targetMBPerSec > MIN_MERGE_MB_PER_SEC) {
            return Math.max(MIN_MERGE_MB_PER_SEC, targetMBPerSec / 1.1);
        } else if (activeThrottledMergeTasksAcrossSchedulersSet.size() > poolSize * 4 && targetMBPerSec < MAX_MERGE_MB_PER_SEC) {
            return Math.min(MAX_MERGE_MB_PER_SEC, targetMBPerSec * 1.1);
        }
        return targetMBPerSec;
    }

    private static synchronized boolean trackNewActiveThrottledMergeTask(MergeTask newMergeTask, int poolSize) {
        assert newMergeTask.isAutoThrottle : "only tracking throttled merge tasks";
        if (activeThrottledMergeTasksAcrossSchedulersSet.add(newMergeTask)) {
            double newTargetMBPerSec = maybeUpdateTargetMBPerSec(poolSize);
            if (newTargetMBPerSec != targetMBPerSec) {
                targetMBPerSec = newTargetMBPerSec;
                for (MergeTask mergeTask : activeThrottledMergeTasksAcrossSchedulersSet) {
                    assert mergeTask.isAutoThrottle;
                    mergeTask.rateLimiter.setMBPerSec(targetMBPerSec);
                }
            }
            return true;
        }
        return false;
    }

    private static synchronized boolean removeFromActiveThrottledMergeTasks(MergeTask doneMergeTask) {
        assert doneMergeTask.isAutoThrottle : "only tracking throttled merge tasks";
        return activeThrottledMergeTasksAcrossSchedulersSet.remove(doneMergeTask);
    }

    private MergeTask newMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, MergeTrigger mergeTrigger) {
        boolean isAutoThrottle = config.isAutoThrottle()
            && mergeTrigger != MergeTrigger.CLOSING
            && merge.getStoreMergeInfo().mergeMaxNumSegments() == -1; // i.e. is NOT a force merge
        return new MergeTask(mergeSource, merge, isAutoThrottle, "Lucene Merge #" + mergeTaskCount.incrementAndGet());
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
        RateLimiter rateLimiter = onGoingMergeRateLimiter.get();
        return new FilterDirectory(in) {
            @Override
            public IndexOutput createOutput(String name, IOContext context) throws IOException {
                ensureOpen();

                // This Directory is only supposed to be used during merging,
                // so all writes should have MERGE context, else there is a bug
                // somewhere that is failing to pass down the right IOContext:
                assert context.context() == IOContext.Context.MERGE : "got context=" + context.context();

                return new RateLimitedIndexOutput(rateLimiter, in.createOutput(name, context));
            }
        };
    }

    final class MergeTask extends AbstractRunnable implements Comparable<MergeTask> {
        private final String name;
        private final SetOnce<Long> mergeStartTimeNS;
        private final MergeSource mergeSource;
        private final OnGoingMerge onGoingMerge;
        private final MergeRateLimiter rateLimiter;
        private final boolean isAutoThrottle;

        MergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, boolean isAutoThrottle, String name) {
            this.name = name;
            this.mergeStartTimeNS = new SetOnce<>();
            this.mergeSource = mergeSource;
            this.onGoingMerge = new OnGoingMerge(merge);
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
            this.isAutoThrottle = isAutoThrottle;
            if (isAutoThrottle) {
                this.rateLimiter.setMBPerSec(targetMBPerSec);
            } else {
                this.rateLimiter.setMBPerSec(Double.POSITIVE_INFINITY);
            }
        }

        @Override
        public int compareTo(MergeTask other) {
            // sort smaller merges (per shard) first, so they are completed before larger ones
            return Long.compare(onGoingMerge.getMerge().estimatedMergeBytes, other.onGoingMerge.getMerge().estimatedMergeBytes);
        }

        @Override
        public void doRun() throws Exception {
            assert isAutoThrottle == false || activeThrottledMergeTasksAcrossSchedulersSet.contains(this)
                : "a running throttled merge should already count as an 'active' merge";
            mergeStartTimeNS.set(System.nanoTime());
            try {
                onGoingMergeRateLimiter.set(this.rateLimiter);
                beforeMerge(onGoingMerge);
                mergeTracking.mergeStarted(onGoingMerge);
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s start", getName()));
                }
                doMerge(mergeSource, onGoingMerge.getMerge());
                if (verbose()) {
                    message(
                        String.format(
                            Locale.ROOT,
                            "merge task %s merge segment [%s] done estSize=%.1f MB (written=%.1f MB) "
                                + "runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s",
                            getName(),
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
            assert onGoingMerge.getMerge().isAborted()
                || isAutoThrottle == false
                || activeThrottledMergeTasksAcrossSchedulersSet.contains(this)
                : "onAfter should always be invoked on aborted or active (and run) merges";
            assert this.mergeStartTimeNS.get() != null : "onAfter should always be invoked after doRun";
            try {
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", getName()));
                }
                afterMerge(onGoingMerge);
            } finally {
                onGoingMergeRateLimiter.remove();
                long tookMS = TimeValue.nsecToMSec(System.nanoTime() - mergeStartTimeNS.get());
                try {
                    mergeTracking.mergeFinished(onGoingMerge.getMerge(), onGoingMerge, tookMS);
                } finally {
                    if (isAutoThrottle) {
                        removeFromActiveThrottledMergeTasks(this);
                    }
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
            if (isAutoThrottle) {
                removeFromActiveThrottledMergeTasks(this);
            }
            // most commonly the merge should've already be aborted by now,
            // plus the engine is probably going to be failed when any merge fails,
            // but keep this in case something believes calling `MergeTask#onFailure` is a sane way to abort a merge
            abortOnGoingMerge();
            mergeDone(this);
            handleMergeException(ExceptionWrappingError.maybeUnwrapCause(e));
        }

        @Override
        public void onRejection(Exception e) {
            assert isAutoThrottle == false || activeThrottledMergeTasksAcrossSchedulersSet.contains(this)
                : "only an 'active' merge can be rejected by the thread pool";
            if (isAutoThrottle) {
                removeFromActiveThrottledMergeTasks(this);
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

        private String getName() {
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
