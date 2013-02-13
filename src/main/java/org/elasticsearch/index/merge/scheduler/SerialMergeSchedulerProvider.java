/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.TrackingSerialMergeScheduler;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 *
 */
public class SerialMergeSchedulerProvider extends AbstractIndexShardComponent implements MergeSchedulerProvider {

    private Set<CustomSerialMergeScheduler> schedulers = new CopyOnWriteArraySet<CustomSerialMergeScheduler>();

    @Inject
    public SerialMergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings) {
        super(shardId, indexSettings);
        logger.trace("using [serial] merge scheduler");
    }

    @Override
    public MergeScheduler newMergeScheduler() {
        CustomSerialMergeScheduler scheduler = new CustomSerialMergeScheduler(logger, this);
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

    public static class CustomSerialMergeScheduler extends TrackingSerialMergeScheduler {

        private final SerialMergeSchedulerProvider provider;

        public CustomSerialMergeScheduler(ESLogger logger, SerialMergeSchedulerProvider provider) {
            super(logger);
            this.provider = provider;
        }

        @Override
        public void merge(IndexWriter writer) throws CorruptIndexException, IOException {
            try {
                super.merge(writer);
            } catch (IOException e) {
                logger.warn("failed to merge", e);
                throw e;
            }
        }

        @Override
        public void close() {
            super.close();
            provider.schedulers.remove(this);
        }
    }
}
