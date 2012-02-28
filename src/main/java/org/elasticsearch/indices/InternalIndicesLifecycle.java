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

package org.elasticsearch.indices;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class InternalIndicesLifecycle extends AbstractComponent implements IndicesLifecycle {

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<Listener>();

    @Inject
    public InternalIndicesLifecycle(Settings settings) {
        super(settings);
    }

    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    public void removeListener(Listener listener) {
        listeners.remove(listener);
    }

    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        for (Listener listener : listeners) {
            listener.shardRoutingChanged(indexShard, oldRouting, newRouting);
        }
    }

    public void beforeIndexCreated(Index index) {
        for (Listener listener : listeners) {
            listener.beforeIndexCreated(index);
        }
    }

    public void afterIndexCreated(IndexService indexService) {
        for (Listener listener : listeners) {
            listener.afterIndexCreated(indexService);
        }
    }

    public void beforeIndexShardCreated(ShardId shardId) {
        for (Listener listener : listeners) {
            listener.beforeIndexShardCreated(shardId);
        }
    }

    public void afterIndexShardCreated(IndexShard indexShard) {
        for (Listener listener : listeners) {
            listener.afterIndexShardCreated(indexShard);
        }
    }

    public void afterIndexShardStarted(IndexShard indexShard) {
        for (Listener listener : listeners) {
            listener.afterIndexShardStarted(indexShard);
        }
    }

    public void beforeIndexClosed(IndexService indexService, boolean delete) {
        for (Listener listener : listeners) {
            listener.beforeIndexClosed(indexService, delete);
        }
    }

    public void afterIndexClosed(Index index, boolean delete) {
        for (Listener listener : listeners) {
            listener.afterIndexClosed(index, delete);
        }
    }

    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, boolean delete) {
        for (Listener listener : listeners) {
            listener.beforeIndexShardClosed(shardId, indexShard, delete);
        }
    }

    public void afterIndexShardClosed(ShardId shardId, boolean delete) {
        for (Listener listener : listeners) {
            listener.afterIndexShardClosed(shardId, delete);
        }
    }
}
