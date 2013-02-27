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

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public abstract class MergeSchedulerProvider<T extends MergeScheduler> extends AbstractIndexShardComponent implements IndexShardComponent {

    public static interface FailureListener {
        void onFailedMerge(MergePolicy.MergeException e);
    }

    private final ThreadPool threadPool;
    private final CopyOnWriteArrayList<FailureListener> failureListeners = new CopyOnWriteArrayList<FailureListener>();

    private final boolean notifyOnMergeFailure;

    protected MergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.notifyOnMergeFailure = componentSettings.getAsBoolean("notify_on_failure", true);
    }

    public void addFailureListener(FailureListener listener) {
        failureListeners.add(listener);
    }

    protected void failedMerge(final MergePolicy.MergeException e) {
        if (!notifyOnMergeFailure) {
            return;
        }
        for (final FailureListener failureListener : failureListeners) {
            threadPool.generic().execute(new Runnable() {
                @Override
                public void run() {
                    failureListener.onFailedMerge(e);
                }
            });
        }
    }

    public abstract T newMergeScheduler();

    public abstract MergeStats stats();
}
