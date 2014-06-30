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

import org.apache.lucene.index.*;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import com.google.common.collect.ImmutableSet;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @deprecated This provider just provides ConcurrentMergeScheduler, and is removed in master.
 */
@Deprecated
public class SerialMergeSchedulerProvider extends MergeSchedulerProvider {
    public static final int DEFAULT_MAX_MERGE_AT_ONCE = 5;

    private Set<CustomSerialMergeScheduler> schedulers = new CopyOnWriteArraySet<>();

    @Inject
    public SerialMergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(shardId, indexSettings, threadPool);
        Integer value = componentSettings.getAsInt("max_merge_at_once", null);
        if (value != null) {
            logger.warn("ignoring index.merge.scheduler.max_merge_at_once [{}], because we are using ConcurrentMergeScheduler(2, 1)", value);
        }
        logger.trace("using [concurrent] merge scheduler, max_thread_count=1, max_merge_count=2");
    }

    @Override
    public int getMaxMerges() {
        return 2;
    }

    @Override
    public MergeScheduler buildMergeScheduler() {
        CustomSerialMergeScheduler scheduler = new CustomSerialMergeScheduler(logger, shardId, this);
        scheduler.setMaxMergesAndThreads(2, 1); // we add max merge count = 2 to make sure we stall once a second merge comes in.
        schedulers.add(scheduler);
        return scheduler;
    }

    @Override
    public MergeStats stats() {
        MergeStats mergeStats = new MergeStats();
        for (CustomSerialMergeScheduler scheduler : schedulers) {
            mergeStats.add(scheduler.totalMerges(), scheduler.totalMergeTime(), scheduler.totalMergeNumDocs(), scheduler.totalMergeSizeInBytes(),
                    scheduler.currentMerges(), scheduler.currentMergesNumDocs(), scheduler.currentMergesSizeInBytes());
        }
        return mergeStats;
    }

    @Override
    public Set<OnGoingMerge> onGoingMerges() {
        for (CustomSerialMergeScheduler scheduler : schedulers) {
            return scheduler.onGoingMerges();
        }
        return ImmutableSet.of();
    }

    @Override
    public void close() {

    }

    /** NOTE: subclasses TrackingCONCURRENTMergeScheduler, but we set max_merge_count = max_thread_count = 1 above */
    public static class CustomSerialMergeScheduler extends TrackingConcurrentMergeScheduler {

        private final ShardId shardId;

        private final SerialMergeSchedulerProvider provider;

        private CustomSerialMergeScheduler(ESLogger logger, ShardId shardId, SerialMergeSchedulerProvider provider) {
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
}
