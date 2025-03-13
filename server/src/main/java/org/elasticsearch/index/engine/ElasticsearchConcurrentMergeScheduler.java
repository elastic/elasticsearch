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
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.util.SameThreadExecutorService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.Executor;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 */
public class ElasticsearchConcurrentMergeScheduler extends ConcurrentMergeScheduler implements ElasticsearchMergeScheduler {

    protected final Logger logger;
    private final Settings indexSettings;
    private final ShardId shardId;

    private final MergeTracking mergeTracking;
    private final MergeSchedulerConfig config;
    private final SameThreadExecutorService sameThreadExecutorService = new SameThreadExecutorService();

    ElasticsearchConcurrentMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
        this.config = indexSettings.getMergeSchedulerConfig();
        this.shardId = shardId;
        this.indexSettings = indexSettings.getSettings();
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergeTracking = new MergeTracking(
            logger,
            () -> indexSettings.getMergeSchedulerConfig().isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY
        );
        refreshConfig();
    }

    @Override
    public Set<OnGoingMerge> onGoingMerges() {
        return mergeTracking.onGoingMerges();
    }

    /** We're currently only interested in messages with this prefix. */
    private static final String MERGE_THREAD_MESSAGE_PREFIX = "merge thread";

    @Override
    // Overridden until investigation in https://github.com/apache/lucene/pull/13475 is complete
    public Executor getIntraMergeExecutor(MergePolicy.OneMerge merge) {
        return sameThreadExecutorService;
    }

    @Override
    // Overridden until investigation in https://github.com/apache/lucene/pull/13475 is complete
    public void close() throws IOException {
        super.close();
        sameThreadExecutorService.shutdown();
    }

    @Override
    /** Overridden to route specific MergeThread messages to our logger. */
    protected boolean verbose() {
        if (logger.isTraceEnabled() && Thread.currentThread() instanceof MergeThread) {
            return true;
        }
        return super.verbose();
    }

    @Override
    /** Overridden to route specific MergeThread messages to our logger. */
    protected void message(String message) {
        if (logger.isTraceEnabled() && Thread.currentThread() instanceof MergeThread && message.startsWith(MERGE_THREAD_MESSAGE_PREFIX)) {
            logger.trace("{}", message);
        }
        super.message(message);
    }

    @Override
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        long timeNS = System.nanoTime();
        OnGoingMerge onGoingMerge = new OnGoingMerge(merge);
        mergeTracking.mergeStarted(onGoingMerge);
        try {
            beforeMerge(onGoingMerge);
            super.doMerge(mergeSource, merge);
        } finally {
            long tookMS = TimeValue.nsecToMSec(System.nanoTime() - timeNS);
            mergeTracking.mergeFinished(merge, onGoingMerge, tookMS);

            afterMerge(onGoingMerge);
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
        thread.setName(
            EsExecutors.threadName(indexSettings, "[" + shardId.getIndexName() + "][" + shardId.id() + "]: " + thread.getName())
        );
        return thread;
    }

    @Override
    public MergeStats stats() {
        return mergeTracking.stats();
    }

    @Override
    public void refreshConfig() {
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

    @Override
    public MergeScheduler getMergeScheduler() {
        return this;
    }
}
