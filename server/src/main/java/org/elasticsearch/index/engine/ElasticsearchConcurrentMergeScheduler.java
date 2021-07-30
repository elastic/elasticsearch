/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.OneMergeHelper;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.metrics.MeanMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 */
class ElasticsearchConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    protected final Logger logger;
    private final Settings indexSettings;
    private final ShardId shardId;

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric currentMergesNumDocs = new CounterMetric();
    private final CounterMetric currentMergesSizeInBytes = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();

    private final Set<OnGoingMerge> onGoingMerges = ConcurrentCollections.newConcurrentSet();
    private final Set<OnGoingMerge> readOnlyOnGoingMerges = Collections.unmodifiableSet(onGoingMerges);
    private final MergeSchedulerConfig config;

    ElasticsearchConcurrentMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
        this.config = indexSettings.getMergeSchedulerConfig();
        this.shardId = shardId;
        this.indexSettings = indexSettings.getSettings();
        this.logger = Loggers.getLogger(getClass(), shardId);
        refreshConfig();
    }

    public Set<OnGoingMerge> onGoingMerges() {
        return readOnlyOnGoingMerges;
    }

    @Override
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        int totalNumDocs = merge.totalNumDocs();
        long totalSizeInBytes = merge.totalBytesSize();
        long timeNS = System.nanoTime();
        currentMerges.inc();
        currentMergesNumDocs.inc(totalNumDocs);
        currentMergesSizeInBytes.inc(totalSizeInBytes);

        OnGoingMerge onGoingMerge = new OnGoingMerge(merge);
        onGoingMerges.add(onGoingMerge);

        if (logger.isTraceEnabled()) {
            logger.trace("merge [{}] starting..., merging [{}] segments, [{}] docs, [{}] size, into [{}] estimated_size",
                OneMergeHelper.getSegmentName(merge), merge.segments.size(), totalNumDocs, new ByteSizeValue(totalSizeInBytes),
                new ByteSizeValue(merge.estimatedMergeBytes));
        }
        try {
            beforeMerge(onGoingMerge);
            super.doMerge(mergeSource, merge);
        } finally {
            long tookMS = TimeValue.nsecToMSec(System.nanoTime() - timeNS);

            onGoingMerges.remove(onGoingMerge);
            afterMerge(onGoingMerge);

            currentMerges.dec();
            currentMergesNumDocs.dec(totalNumDocs);
            currentMergesSizeInBytes.dec(totalSizeInBytes);

            totalMergesNumDocs.inc(totalNumDocs);
            totalMergesSizeInBytes.inc(totalSizeInBytes);
            totalMerges.inc(tookMS);
            long stoppedMS = TimeValue.nsecToMSec(
                merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.STOPPED)
            );
            long throttledMS = TimeValue.nsecToMSec(
                merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.PAUSED)
            );
            final Thread thread = Thread.currentThread();
            long totalBytesWritten = OneMergeHelper.getTotalBytesWritten(thread, merge);
            double mbPerSec = OneMergeHelper.getMbPerSec(thread, merge);
            totalMergeStoppedTime.inc(stoppedMS);
            totalMergeThrottledTime.inc(throttledMS);

            String message = String.format(Locale.ROOT,
                                           "merge segment [%s] done: took [%s], [%,.1f MB], [%,d docs], [%s stopped], " +
                                               "[%s throttled], [%,.1f MB written], [%,.1f MB/sec throttle]",
                                           OneMergeHelper.getSegmentName(merge),
                                           TimeValue.timeValueMillis(tookMS),
                                           totalSizeInBytes/1024f/1024f,
                                           totalNumDocs,
                                           TimeValue.timeValueMillis(stoppedMS),
                                           TimeValue.timeValueMillis(throttledMS),
                                           totalBytesWritten/1024f/1024f,
                                           mbPerSec);

            if (tookMS > 20000) { // if more than 20 seconds, DEBUG log it
                logger.debug("{}", message);
            } else if (logger.isTraceEnabled()) {
                logger.trace("{}", message);
            }
        }
    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void beforeMerge(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void afterMerge(OnGoingMerge merge) {}

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }

    @Override
    protected boolean maybeStall(MergeSource mergeSource) {
        // Don't stall here, because we do our own index throttling (in InternalEngine.IndexThrottle) when merges can't keep up
        return true;
    }

    @Override
    protected MergeThread getMergeThread(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        MergeThread thread = super.getMergeThread(mergeSource, merge);
        thread.setName(EsExecutors.threadName(indexSettings, "[" + shardId.getIndexName() + "][" + shardId.id() + "]: " +
            thread.getName()));
        return thread;
    }

    MergeStats stats() {
        final MergeStats mergeStats = new MergeStats();
        mergeStats.add(totalMerges.count(), totalMerges.sum(), totalMergesNumDocs.count(), totalMergesSizeInBytes.count(),
                currentMerges.count(), currentMergesNumDocs.count(), currentMergesSizeInBytes.count(),
                totalMergeStoppedTime.count(),
                totalMergeThrottledTime.count(),
                config.isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY);
        return mergeStats;
    }

    void refreshConfig() {
        if (this.getMaxMergeCount() != config.getMaxMergeCount() || this.getMaxThreadCount() != config.getMaxThreadCount()) {
            this.setMaxMergesAndThreads(config.getMaxMergeCount(), config.getMaxThreadCount());
        }
        boolean isEnabled = getIORateLimitMBPerSec() != Double.POSITIVE_INFINITY;
        if (config.isAutoThrottle() && isEnabled == false) {
            enableAutoIOThrottle();
        } else if (config.isAutoThrottle() == false && isEnabled) {
            disableAutoIOThrottle();
        }
    }

}
