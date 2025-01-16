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
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class ExecutorMergeScheduler extends MergeScheduler implements ElasticsearchMergeScheduler {

    private final MergeSchedulerConfig config;
    private final Logger logger;
    private final MergeTracking mergeTracking;
    private final ExecutorService executorService;
    private final ThreadLocal<MergeRateLimiter> onGoingMergeRateLimiter = new ThreadLocal<>();

    public ExecutorMergeScheduler(ShardId shardId, IndexSettings indexSettings, ExecutorService executorService) {
        this.config = indexSettings.getMergeSchedulerConfig();
        this.logger = Loggers.getLogger(getClass(), shardId);
        // TODO: use real IO rate here
        this.mergeTracking = new MergeTracking(logger, () -> Double.POSITIVE_INFINITY);
        this.executorService = executorService;
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
        // No-op
    }

    @Override
    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
        MergePolicy.OneMerge merge = mergeSource.getNextMerge();
        if (merge != null) {
            submitNewMergeTask(mergeSource, merge);
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

    public synchronized int getMaxMergeCount() {
        return config.getMaxMergeCount();
    }

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }

    protected void handleMergeException(Throwable exc) {
        if (exc instanceof MergePolicy.MergeException mergeException) {
            throw mergeException;
        } else {
            throw new MergePolicy.MergeException(exc);
        }
    }

    private void submitNewMergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge) {
        MergeTask mergeTask = mergeTask(mergeSource, merge);
        executorService.execute(mergeTask);
    }

    private MergeTask mergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge) {
        return new MergeTask(mergeSource, merge, "TODO");
    }

    @Override
    /** Overridden to route messages to our logger too, in addition to the {@link org.apache.lucene.util.InfoStream} that lucene uses. */
    protected boolean verbose() {
        if (logger.isTraceEnabled()) {
            return true;
        }
        return super.verbose();
    }

    @Override
    /** Overridden to route messages to our logger too, in addition to the {@link org.apache.lucene.util.InfoStream} that lucene uses. */
    protected void message(String message) {
        if (logger.isTraceEnabled()) {
            logger.trace("{}", message);
        }
        super.message(message);
    }

    /**
     * Does the actual merge, by calling {@link org.apache.lucene.index.MergeScheduler.MergeSource#merge}
     */
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        mergeSource.merge(merge);
    }

    protected class MergeTask extends AbstractRunnable implements Comparable<MergeTask> {
        private final MergeSource mergeSource;
        private final OnGoingMerge onGoingMerge;
        private final MergeRateLimiter rateLimiter;
        private final String name;

        public MergeTask(MergeSource mergeSource, MergePolicy.OneMerge merge, String name) {
            this.mergeSource = mergeSource;
            this.onGoingMerge = new OnGoingMerge(merge);
            this.rateLimiter = new MergeRateLimiter(merge.getMergeProgress());
            this.name = name;
        }

        @Override
        public int compareTo(MergeTask other) {
            // sort smaller merges (per shard) first, so they are completed before larger ones
            return Long.compare(onGoingMerge.getMerge().estimatedMergeBytes, other.onGoingMerge.getMerge().estimatedMergeBytes);
        }

        @Override
        public void doRun() {
            final long startTimeNS = System.nanoTime();
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
                                    "merge task %s merge segment [%s] done estSize=%.1f MB (written=%.1f MB) runTime=%.1fs (stopped=%.1fs, paused=%.1fs) rate=%s",
                                    getName(),
                                    getSegmentName(onGoingMerge.getMerge()),
                                    bytesToMB(onGoingMerge.getMerge().estimatedMergeBytes),
                                    bytesToMB(rateLimiter.getTotalBytesWritten()),
                                    nsToSec(System.nanoTime() - startTimeNS),
                                    nsToSec(rateLimiter.getTotalStoppedNS()),
                                    nsToSec(rateLimiter.getTotalPausedNS()),
                                    rateToString(rateLimiter.getMBPerSec())));
                }
                if (verbose()) {
                    message(String.format(Locale.ROOT, "merge task %s end", getName()));
                }
            } catch (Throwable exc) {
                if (exc instanceof MergePolicy.MergeAbortedException) {
                    // OK to ignore. This is what Lucene's ConcurrentMergeScheduler does
                } else {
                    handleMergeException(exc);
                }
            } finally {
                try {
                    afterMerge(onGoingMerge);
                } finally {
                    onGoingMergeRateLimiter.remove();
                    long tookMS = TimeValue.nsecToMSec(System.nanoTime() - startTimeNS);
                    mergeTracking.mergeFinished(onGoingMerge.getMerge(), onGoingMerge, tookMS);
                }
            }
        }

        @Override
        public void onAfter() {
            MergePolicy.OneMerge nextMerge;
            try {
                nextMerge = mergeSource.getNextMerge();
            } catch (IllegalStateException e) {
                if (verbose()) {
                    message("merge task poll failed, likely that index writer is failed");
                }
                return; // ignore exception, we expect the IW failure to be logged elsewhere
            }
            if (nextMerge != null) {
                submitNewMergeTask(mergeSource, nextMerge);
            }
        }

        @Override
        public void onFailure(Exception e) {
            // doRun already handles exceptions, this is just to be extra defensive from any future code modifications
            handleMergeException(e);
        }

        @Override
        public void onRejection(Exception e) {
            if (verbose()) {
                message(String.format(Locale.ROOT, "merge task [%s] rejected by thread pool, aborting", onGoingMerge.getId()));
            }
            abortOnGoingMerge();
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
