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
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

/**
 */
public class InternalIndicesWarmer extends AbstractComponent implements IndicesWarmer {

    private final ClusterService clusterService;

    private final IndicesService indicesService;

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject
    public InternalIndicesWarmer(Settings settings, ClusterService clusterService, IndicesService indicesService) {
        super(settings);
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

    public void warm(ShardId shardId, Engine.Searcher searcher) {
        IndexMetaData indexMetaData = clusterService.state().metaData().index(shardId.index().name());
        if (indexMetaData == null) {
            return;
        }
        if (!indexMetaData.settings().getAsBoolean("index.warm.enabled", true)) {
            return;
        }
        IndexService indexService = indicesService.indexService(shardId.index().name());
        if (indexService == null) {
            return;
        }
        IndexShard indexShard = indexService.shard(shardId.id());
        if (indexShard == null) {
            return;
        }
        if (logger.isTraceEnabled()) {
            logger.trace("[{}][{}] warming [{}]", shardId.index().name(), shardId.id(), searcher.reader());
        }
        indexShard.warmerService().onPreWarm();
        long time = System.nanoTime();
        for (Listener listener : listeners) {
            try {
                listener.warm(shardId, indexMetaData, searcher);
            } catch (Exception e) {
                logger.warn("[{}][{}] failed to warm [{}]", shardId.index().name(), shardId.id(), listener);
            }
        }
        long took = System.nanoTime() - time;
        indexShard.warmerService().onPostWarm(took);
        if (logger.isTraceEnabled()) {
            logger.trace("[{}][{}] warming took [{}]", shardId.index().name(), shardId.id(), new TimeValue(took, TimeUnit.NANOSECONDS));
        }
    }
}
