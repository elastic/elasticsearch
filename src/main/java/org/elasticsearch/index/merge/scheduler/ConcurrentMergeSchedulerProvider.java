/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.merge.scheduler;

import com.google.common.collect.ImmutableSet;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.TrackingConcurrentMergeScheduler;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.settings.IndexSettingsService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class ConcurrentMergeSchedulerProvider extends MergeSchedulerProvider {

    private final IndexSettingsService indexSettingsService;
    private final ApplySettings applySettings = new ApplySettings();

    public static final String MAX_THREAD_COUNT = "index.merge.scheduler.max_thread_count";
    public static final String MAX_MERGE_COUNT = "index.merge.scheduler.max_merge_count";
    public static final String AUTO_THROTTLE = "index.merge.scheduler.auto_throttle";

    private volatile int maxThreadCount;
    private volatile int maxMergeCount;
    private volatile boolean autoThrottle;

    private Set<CustomConcurrentMergeScheduler> schedulers = new CopyOnWriteArraySet<>();

    @Inject
    public ConcurrentMergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexSettingsService indexSettingsService) {
        super(shardId, indexSettings, threadPool);
        this.indexSettingsService = indexSettingsService;
        this.maxThreadCount = indexSettings.getAsInt(MAX_THREAD_COUNT, Math.max(1, Math.min(4, EsExecutors.boundedNumberOfProcessors(indexSettings) / 2)));
        this.maxMergeCount = indexSettings.getAsInt(MAX_MERGE_COUNT, maxThreadCount + 5);
        this.autoThrottle = indexSettings.getAsBoolean(AUTO_THROTTLE, true);
        logger.debug("using [concurrent] merge scheduler with max_thread_count[{}], max_merge_count[{}], auto_throttle[{}]", maxThreadCount, maxMergeCount, autoThrottle);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public MergeScheduler newMergeScheduler() {
        CustomConcurrentMergeScheduler concurrentMergeScheduler = new CustomConcurrentMergeScheduler(logger, shardId, this);
        // NOTE: we pass maxMergeCount+1 here so that CMS will allow one too many merges to kick off which then allows
        // InternalEngine.IndexThrottle to detect too-many-merges and throttle:
        concurrentMergeScheduler.setMaxMergesAndThreads(maxMergeCount+1, maxThreadCount);
        if (autoThrottle) {
            concurrentMergeScheduler.enableAutoIOThrottle();
        } else {
            concurrentMergeScheduler.disableAutoIOThrottle();
        }
        schedulers.add(concurrentMergeScheduler);
        return concurrentMergeScheduler;
    }

    @Override
    public MergeStats stats() {
        MergeStats mergeStats = new MergeStats();
        // TODO: why would there be more than one CMS for a single shard...?
        for (CustomConcurrentMergeScheduler scheduler : schedulers) {
            mergeStats.add(scheduler.totalMerges(), scheduler.totalMergeTime(), scheduler.totalMergeNumDocs(), scheduler.totalMergeSizeInBytes(),
                           scheduler.currentMerges(), scheduler.currentMergesNumDocs(), scheduler.currentMergesSizeInBytes(),
                           scheduler.totalMergeStoppedTimeMillis(),
                           scheduler.totalMergeThrottledTimeMillis(),
                           autoThrottle ? scheduler.getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY);
        }
        return mergeStats;
    }

    @Override
    public Set<OnGoingMerge> onGoingMerges() {
        for (CustomConcurrentMergeScheduler scheduler : schedulers) {
            return scheduler.onGoingMerges();
        }
        return ImmutableSet.of();
    }

    @Override
    public void close() {
        indexSettingsService.removeListener(applySettings);
    }

    @Override
    public int getMaxMerges() {
        return this.maxMergeCount;
    }

    public static class CustomConcurrentMergeScheduler extends TrackingConcurrentMergeScheduler {

        private final ShardId shardId;

        private final ConcurrentMergeSchedulerProvider provider;

        private CustomConcurrentMergeScheduler(ESLogger logger, ShardId shardId, ConcurrentMergeSchedulerProvider provider) {
            super(logger);
            this.shardId = shardId;
            this.provider = provider;
        }

        @Override
        protected MergeThread getMergeThread(IndexWriter writer, MergePolicy.OneMerge merge) throws IOException {
            MergeThread thread = super.getMergeThread(writer, merge);
            thread.setName(EsExecutors.threadName(provider.indexSettings(), "[" + shardId.index().name() + "][" + shardId.id() + "]: " + thread.getName()));
            return thread;
        }

        @Override
        protected void handleMergeException(Directory dir, Throwable exc) {
            logger.error("failed to merge", exc);
            provider.failedMerge(new MergePolicy.MergeException(exc, dir));
            // NOTE: do not call super.handleMergeException here, which would just re-throw the exception
            // and let Java's thread exc handler see it / log it to stderr, but we already 1) logged it
            // and 2) handled the exception by failing the engine
        }

        @Override
        public void close() {
            super.close();
            provider.schedulers.remove(this);
        }

        @Override
        protected void beforeMerge(OnGoingMerge merge) {
            super.beforeMerge(merge);
            provider.beforeMerge(merge);
        }

        @Override
        protected void afterMerge(OnGoingMerge merge) {
            super.afterMerge(merge);
            provider.afterMerge(merge);
        }

        @Override
        protected boolean maybeStall(IndexWriter writer) {
            // Don't stall here, because we do our own index throttling (in InternalEngine.IndexThrottle) when merges can't keep up
            return true;
        }
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int maxThreadCount = settings.getAsInt(MAX_THREAD_COUNT, ConcurrentMergeSchedulerProvider.this.maxThreadCount);
            if (maxThreadCount != ConcurrentMergeSchedulerProvider.this.maxThreadCount) {
                logger.info("updating [{}] from [{}] to [{}]", MAX_THREAD_COUNT, ConcurrentMergeSchedulerProvider.this.maxThreadCount, maxThreadCount);
                ConcurrentMergeSchedulerProvider.this.maxThreadCount = maxThreadCount;
                for (CustomConcurrentMergeScheduler scheduler : schedulers) {
                    scheduler.setMaxMergesAndThreads(ConcurrentMergeSchedulerProvider.this.maxMergeCount, maxThreadCount);
                }
            }

            int maxMergeCount = settings.getAsInt(MAX_MERGE_COUNT, ConcurrentMergeSchedulerProvider.this.maxMergeCount);
            if (maxMergeCount != ConcurrentMergeSchedulerProvider.this.maxMergeCount) {
                logger.info("updating [{}] from [{}] to [{}]", MAX_MERGE_COUNT, ConcurrentMergeSchedulerProvider.this.maxMergeCount, maxMergeCount);
                ConcurrentMergeSchedulerProvider.this.maxMergeCount = maxMergeCount;
                for (CustomConcurrentMergeScheduler scheduler : schedulers) {
                    scheduler.setMaxMergesAndThreads(maxMergeCount, ConcurrentMergeSchedulerProvider.this.maxThreadCount);
                }
            }

            boolean autoThrottle = settings.getAsBoolean(AUTO_THROTTLE, ConcurrentMergeSchedulerProvider.this.autoThrottle);
            if (autoThrottle != ConcurrentMergeSchedulerProvider.this.autoThrottle) {
                logger.info("updating [{}] from [{}] to [{}]", AUTO_THROTTLE, ConcurrentMergeSchedulerProvider.this.autoThrottle, autoThrottle);
                ConcurrentMergeSchedulerProvider.this.autoThrottle = autoThrottle;
                for (CustomConcurrentMergeScheduler scheduler : schedulers) {
                    if (autoThrottle) {
                        scheduler.enableAutoIOThrottle();
                    } else {
                        scheduler.disableAutoIOThrottle();
                    }
                }
            }
        }
    }
}
