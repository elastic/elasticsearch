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

package org.elasticsearch.indices.warmer;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 */
public class InternalIndicesWarmer extends AbstractComponent implements IndicesWarmer {

    public static final String INDEX_WARMER_ENABLED = "index.warmer.enabled";

    private final ThreadPool threadPool;

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject
    public InternalIndicesWarmer(Settings settings, ThreadPool threadPool, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.indicesService = indicesService;
    }

    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void warm(final WarmerContext context) {
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
            logger.trace("[{}][{}] warming [{}]", context.shardId().index().name(), context.shardId().id(), context.newSearcher().reader());
        }
        indexShard.warmerService().onPreWarm();
        long time = System.nanoTime();
        for (final Listener listener : listeners) {
            listener.warm(indexShard, indexMetaData, context, threadPool);
        }
        long took = System.nanoTime() - time;
        indexShard.warmerService().onPostWarm(took);
        if (indexShard.warmerService().logger().isTraceEnabled()) {
            indexShard.warmerService().logger().trace("warming took [{}]", new TimeValue(took, TimeUnit.NANOSECONDS));
        }
    }
}
