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

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;

import java.util.Collection;
import java.util.List;

/**
 * A composite {@link IndexEventListener} that forwards all callbacks to an immutable list of IndexEventListener
 */
final class CompositeIndexEventListener implements IndexEventListener {

    private final List<IndexEventListener> listeners;
    private final Logger logger;

    CompositeIndexEventListener(IndexSettings indexSettings, Collection<IndexEventListener> listeners) {
        for (IndexEventListener listener : listeners) {
            if (listener == null) {
                throw new IllegalArgumentException("listeners must be non-null");
            }
        }
        this.listeners = List.copyOf(listeners);
        this.logger = Loggers.getLogger(getClass(), indexSettings.getIndex());
    }

    @Override
    public void shardRoutingChanged(IndexShard indexShard, @Nullable ShardRouting oldRouting, ShardRouting newRouting) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.shardRoutingChanged(indexShard, oldRouting, newRouting);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke shard touring changed callback",
                    indexShard.shardId().getId()), e);
            }
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardCreated(indexShard);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard created callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardStarted(indexShard);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard started callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke before shard closed callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                      Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard closed callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }


    @Override
    public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, IndexShardState currentState,
                                       @Nullable String reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.indexShardStateChanged(indexShard, previousState, indexShard.state(), reason);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke index shard state changed callback",
                    indexShard.shardId().getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexCreated(index, indexSettings);
            } catch (Exception e) {
                logger.warn("failed to invoke before index created callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexCreated(indexService);
            } catch (Exception e) {
                logger.warn("failed to invoke after index created callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardCreated(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardCreated(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() ->
                    new ParameterizedMessage("[{}] failed to invoke before shard created callback", shardId), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexRemoved(indexService, reason);
            } catch (Exception e) {
                logger.warn("failed to invoke before index removed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexRemoved(Index index, IndexSettings indexSettings, IndexRemovalReason reason) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexRemoved(index, indexSettings, reason);
            } catch (Exception e) {
                logger.warn("failed to invoke after index removed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardDeleted(ShardId shardId,
                                        Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke before shard deleted callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId,
                                       Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("[{}] failed to invoke after shard deleted callback",
                    shardId.getId()), e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        for (IndexEventListener listener  : listeners) {
            try {
                listener.beforeIndexAddedToCluster(index, indexSettings);
            } catch (Exception e) {
                logger.warn("failed to invoke before index added to cluster callback", e);
                throw e;
            }
        }
    }

    @Override
    public void onStoreCreated(ShardId shardId) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.onStoreCreated(shardId);
            } catch (Exception e) {
                logger.warn("failed to invoke on store created", e);
                throw e;
            }
        }
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        for (IndexEventListener listener  : listeners) {
            try {
                listener.onStoreClosed(shardId);
            } catch (Exception e) {
                logger.warn("failed to invoke on store closed", e);
                throw e;
            }
        }
    }
}
