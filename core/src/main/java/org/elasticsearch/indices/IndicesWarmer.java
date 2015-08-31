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

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 */
public final class IndicesWarmer extends AbstractComponent {

    public static final String INDEX_WARMER_ENABLED = "index.warmer.enabled";

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public IndicesWarmer(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void warmNewReaders(final WarmerContext context) {
        warmInternal(context, false);
    }

    public void warmTopReader(WarmerContext context) {
        warmInternal(context, true);
    }

    private void warmInternal(final WarmerContext context, boolean topReader) {
        final IndexMetaData indexMetaData = clusterService.state().metaData().index(context.shardId().index().name());
        if (indexMetaData == null) {
            return;
        }
        if (!indexMetaData.settings().getAsBoolean(INDEX_WARMER_ENABLED, settings.getAsBoolean(INDEX_WARMER_ENABLED, true))) {
            return;
        }
        IndexService indexService = indicesService.indexService(context.shardId().index().name());
        if (indexService == null) {
            return;
        }
        final IndexShard indexShard = indexService.shard(context.shardId().id());
        if (indexShard == null) {
            return;
        }
        if (logger.isTraceEnabled()) {
            if (topReader) {
                logger.trace("[{}][{}] top warming [{}]", context.shardId().index().name(), context.shardId().id(), context);
            } else {
                logger.trace("[{}][{}] warming [{}]", context.shardId().index().name(), context.shardId().id(), context);
            }
        }
        indexShard.warmerService().onPreWarm();
        long time = System.nanoTime();
        final List<TerminationHandle> terminationHandles = new ArrayList<>();
        // get a handle on pending tasks
        for (final Listener listener : listeners) {
            if (topReader) {
                terminationHandles.add(listener.warmTopReader(indexShard, indexMetaData, context, threadPool));
            } else {
                terminationHandles.add(listener.warmNewReaders(indexShard, indexMetaData, context, threadPool));
            }
        }
        // wait for termination
        for (TerminationHandle terminationHandle : terminationHandles) {
            try {
                terminationHandle.awaitTermination();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (topReader) {
                    logger.warn("top warming has been interrupted", e);
                } else {
                    logger.warn("warming has been interrupted", e);
                }
                break;
            }
        }
        long took = System.nanoTime() - time;
        indexShard.warmerService().onPostWarm(took);
        if (indexShard.warmerService().logger().isTraceEnabled()) {
            if (topReader) {
                indexShard.warmerService().logger().trace("top warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            } else {
                indexShard.warmerService().logger().trace("warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
            }
        }
    }

    /** A handle on the execution of  warm-up action. */
    public interface TerminationHandle {

        public static TerminationHandle NO_WAIT = new TerminationHandle() {
            @Override
            public void awaitTermination() {}
        };

        /** Wait until execution of the warm-up action completes. */
        void awaitTermination() throws InterruptedException;
    }
    public static abstract class Listener {

        public String executor() {
            return ThreadPool.Names.WARMER;
        }

        /** Queue tasks to warm-up the given segments and return handles that allow to wait for termination of the execution of those tasks. */
        public abstract TerminationHandle warmNewReaders(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool);

        public abstract TerminationHandle warmTopReader(IndexShard indexShard, IndexMetaData indexMetaData, WarmerContext context, ThreadPool threadPool);
    }

    public static final class WarmerContext {

        private final ShardId shardId;
        private final Engine.Searcher searcher;

        public WarmerContext(ShardId shardId, Engine.Searcher searcher) {
            this.shardId = shardId;
            this.searcher = searcher;
        }

        public ShardId shardId() {
            return shardId;
        }

        /** Return a searcher instance that only wraps the segments to warm. */
        public Engine.Searcher searcher() {
            return searcher;
        }

        public IndexReader reader() {
            return searcher.reader();
        }

        @Override
        public String toString() {
            return "WarmerContext: " + searcher.reader();
        }
    }
}
