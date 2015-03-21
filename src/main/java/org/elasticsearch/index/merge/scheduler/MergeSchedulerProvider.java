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

import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexShardComponent;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public abstract class MergeSchedulerProvider extends AbstractIndexShardComponent implements Closeable {

    public static interface FailureListener {
        void onFailedMerge(MergePolicy.MergeException e);
    }

    /**
     * Listener for events before/after single merges. Called on the merge thread.
     */
    public static interface Listener {

        /**
         * A callback before a merge is going to execute. Note, any logic here will block the merge
         * till its done.
         */
        void beforeMerge(OnGoingMerge merge);

        /**
         * A callback after a merge is going to execute. Note, any logic here will block the merge
         * thread.
         */
        void afterMerge(OnGoingMerge merge);
    }

    private final ThreadPool threadPool;
    private final CopyOnWriteArrayList<FailureListener> failureListeners = new CopyOnWriteArrayList<>();
    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    private final boolean notifyOnMergeFailure;

    protected MergeSchedulerProvider(ShardId shardId, @IndexSettings Settings indexSettings, ThreadPool threadPool) {
        super(shardId, indexSettings);
        this.threadPool = threadPool;
        this.notifyOnMergeFailure = indexSettings.getAsBoolean("index.merge.scheduler.notify_on_failure", true);
    }

    public void addFailureListener(FailureListener listener) {
        failureListeners.add(listener);
    }

    public void removeFailureListener(FailureListener listener) {
        failureListeners.remove(listener);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
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

    protected void beforeMerge(OnGoingMerge merge) {
        for (Listener listener : listeners) {
            listener.beforeMerge(merge);
        }
    }

    protected void afterMerge(OnGoingMerge merge) {
        for (Listener listener : listeners) {
            listener.afterMerge(merge);
        }
    }

    /** Maximum number of allowed running merges before index throttling kicks in. */
    public abstract int getMaxMerges();

    public abstract MergeScheduler newMergeScheduler();

    public abstract MergeStats stats();

    public abstract Set<OnGoingMerge> onGoingMerges();

    @Override
    public abstract void close();
}
