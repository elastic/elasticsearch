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

    private static final String MAX_THREAD_COUNT_KEY = "max_thread_count";
    private static final String MAX_MERGE_COUNT_KEY = "max_merge_count";

    public static final String MAX_THREAD_COUNT = "index.merge.scheduler." + MAX_THREAD_COUNT_KEY;
    public static final String MAX_MERGE_COUNT = "index.merge.scheduler." + MAX_MERGE_COUNT_KEY;

    private volatile int maxThreadCount;
    private volatile int maxMergeCount;

    private Set<CustomConcurrentMergeScheduler> schedulers = new CopyOnWriteArraySet<>();

    @Inject
    public ConcurrentMergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool, IndexSettingsService indexSettingsService) {
        super(shardId, indexSettings, threadPool);
        this.indexSettingsService = indexSettingsService;
        // TODO LUCENE MONITOR this will change in Lucene 4.0
        this.maxThreadCount = componentSettings.getAsInt(MAX_THREAD_COUNT_KEY, Math.max(1, Math.min(3, EsExecutors.boundedNumberOfProcessors(indexSettings) / 2)));
        this.maxMergeCount = componentSettings.getAsInt(MAX_MERGE_COUNT_KEY, maxThreadCount + 2);
        logger.debug("using [concurrent] merge scheduler with max_thread_count[{}], max_merge_count[{}]", maxThreadCount, maxMergeCount);

        indexSettingsService.addListener(applySettings);
    }

    @Override
    public MergeScheduler buildMergeScheduler() {
        CustomConcurrentMergeScheduler concurrentMergeScheduler = new CustomConcurrentMergeScheduler(logger, shardId, this);
        // which would then stall if there are 2 merges in flight, and unstall once we are back to 1 or 0 merges
        // NOTE: we pass maxMergeCount+1 here so that CMS will allow one too many merges to kick off which then allows
        // InternalEngine.IndexThrottle to detect too-many-merges and throttle:
        concurrentMergeScheduler.setMaxMergesAndThreads(maxMergeCount+1, maxThreadCount);
        schedulers.add(concurrentMergeScheduler);
        return concurrentMergeScheduler;
    }

    @Override
    public MergeStats stats() {
        MergeStats mergeStats = new MergeStats();
        for (CustomConcurrentMergeScheduler scheduler : schedulers) {
            mergeStats.add(scheduler.totalMerges(), scheduler.totalMergeTime(), scheduler.totalMergeNumDocs(), scheduler.totalMergeSizeInBytes(),
                    scheduler.currentMerges(), scheduler.currentMergesNumDocs(), scheduler.currentMergesSizeInBytes());
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
        protected void handleMergeException(Throwable exc) {
            logger.warn("failed to merge", exc);
            provider.failedMerge(new MergePolicy.MergeException(exc, dir));
            super.handleMergeException(exc);
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
    }

    class ApplySettings implements IndexSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            int maxThreadCount = settings.getAsInt("index.merge.scheduler.max_thread_count", ConcurrentMergeSchedulerProvider.this.maxThreadCount);
            if (maxThreadCount != ConcurrentMergeSchedulerProvider.this.maxThreadCount) {
                logger.info("updating [max_thread_count] from [{}] to [{}]", ConcurrentMergeSchedulerProvider.this.maxThreadCount, maxThreadCount);
                ConcurrentMergeSchedulerProvider.this.maxThreadCount = maxThreadCount;
                for (CustomConcurrentMergeScheduler scheduler : schedulers) {
                    scheduler.setMaxMergesAndThreads(ConcurrentMergeSchedulerProvider.this.maxMergeCount, maxThreadCount);
                }
            }

            int maxMergeCount = settings.getAsInt("index.merge.scheduler.max_merge_count", ConcurrentMergeSchedulerProvider.this.maxMergeCount);
            if (maxMergeCount != ConcurrentMergeSchedulerProvider.this.maxMergeCount) {
                logger.info("updating [max_merge_count] from [{}] to [{}]", ConcurrentMergeSchedulerProvider.this.maxMergeCount, maxMergeCount);
                ConcurrentMergeSchedulerProvider.this.maxMergeCount = maxMergeCount;
                for (CustomConcurrentMergeScheduler scheduler : schedulers) {
                    scheduler.setMaxMergesAndThreads(maxMergeCount, ConcurrentMergeSchedulerProvider.this.maxThreadCount);
                }
            }
        }
    }
}
