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

package org.elasticsearch.indices;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 */
public final class IndicesWarmer extends AbstractComponent {

    public static final String INDEX_WARMER_ENABLED = "index.warmer.enabled";

    private final ThreadPool threadPool;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public IndicesWarmer(Settings settings, ThreadPool threadPool) {
        super(settings);
        this.threadPool = threadPool;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void warm(Engine.Searcher searcher, IndexShard shard, IndexSettings settings, boolean isTopReader) {
        if (shard.state() == IndexShardState.CLOSED) {
            return;
        }
        final Settings indexSettings = settings.getSettings();
        if (!indexSettings.getAsBoolean(INDEX_WARMER_ENABLED, settings.getNodeSettings().getAsBoolean(INDEX_WARMER_ENABLED, true))) {
            return;
        }
        if (logger.isTraceEnabled()) {
            if (isTopReader) {
                logger.trace("{} top warming [{}]", shard.shardId(), searcher.reader());
            } else {
                logger.trace("{} warming [{}]", shard.shardId(), searcher.reader());
            }
        }
        shard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            if (isTopReader) {
                terminationHandles.add(listener.warmTopReader(shard, searcher));
            } else {
                terminationHandles.add(listener.warmNewReaders(shard, searcher));
            }
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (isTopReader) {
                    logger.warn("top warming has been interrupted", e);
                } else {
                    logger.warn("warming has been interrupted", e);
                }
                break;
            }
        }
        long took = System.nanoTime() - time;
        shard.warmerService().onPostWarm(took);
        if (shard.warmerService().logger().isTraceEnabled()) {
            if (isTopReader) {
                shard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            } else {
                shard.warmerService().logger().trace("warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            }
        }
    }

    /**
     * Returns an executor for async warmer tasks
     */
    public Executor getExecutor() {
        return threadPool.executor(ThreadPool.Names.WARMER);
    }

    /** A handle on the execution of  warm-up action. */
    public interface TerminationHandle {

        TerminationHandle NO_WAIT = () -> {};

        /** Wait until execution of the warm-up action completes. */
        void awaitTermination() throws InterruptedException;
    }
    public interface Listener {
        /** Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the execution of those tasks. */
        TerminationHandle warmNewReaders(IndexShard indexShard, Engine.Searcher searcher);

        TerminationHandle warmTopReader(IndexShard indexShard, Engine.Searcher searcher);
    }

}
