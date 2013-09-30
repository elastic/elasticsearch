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
            try {
                listener.shardRoutingChanged(indexShard, oldRouting, newRouting);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke shard touring changed callback", t, indexShard.shardId());
            }
        }
    }

    public void beforeIndexCreated(Index index) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexCreated(index);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index created callback", t, index.name());
            }
        }
    }

    public void afterIndexCreated(IndexService indexService) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexCreated(indexService);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke after index created callback", t, indexService.index().name());
            }
        }
    }

    public void beforeIndexShardCreated(ShardId shardId) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardCreated(shardId);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard created callback", t, shardId);
            }
        }
    }

    public void afterIndexShardCreated(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardCreated(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard created callback", t, indexShard.shardId());
            }
        }
    }

    public void afterIndexShardStarted(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardStarted(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard started callback", t, indexShard.shardId());
            }
        }
    }

    public void beforeIndexClosed(IndexService indexService) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexClosed(indexService);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index closed callback", t, indexService.index().name());
            }
        }
    }

    public void afterIndexClosed(Index index) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexClosed(index);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke after index closed callback", t, index.name());
            }
        }
    }

    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardClosed(shardId, indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard closed callback", t, shardId);
            }
        }
    }

    public void afterIndexShardClosed(ShardId shardId) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardClosed(shardId);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard closed callback", t, shardId);
            }
        }
    }
}
