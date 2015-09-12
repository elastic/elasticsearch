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

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.settings.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * InternalIndicesLifecycle handles invoking each listener for the Index. All
 * exceptions thrown by listeners are logged and then re-thrown to stop further
 * index action.
 */
public class InternalIndicesLifecycle extends AbstractComponent implements IndicesLifecycle {

    private final CopyOnWriteArrayList<Listener> listeners = new CopyOnWriteArrayList<>();

    @Inject
    public InternalIndicesLifecycle(Settings settings) {
        super(settings);
    }
    @Override
    public void addListener(Listener listener) {
        listeners.add(listener);
    }

    @Override
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

    public void beforeIndexAddedToCluster(Index index, @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexAddedToCluster(index, indexSettings);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index added to cluster callback", t, index.name());
                throw t;
            }
        }
    }

    public void beforeIndexCreated(Index index, @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexCreated(index, indexSettings);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index created callback", t, index.name());
                throw t;
            }
        }
    }

    public void afterIndexCreated(IndexService indexService) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexCreated(indexService);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke after index created callback", t, indexService.index().name());
                throw t;
            }
        }
    }

    public void beforeIndexShardCreated(ShardId shardId, @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardCreated(shardId, indexSettings);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard created callback", t, shardId);
                throw t;
            }
        }
    }

    public void afterIndexShardCreated(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardCreated(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard created callback", t, indexShard.shardId());
                throw t;
            }
        }
    }

    public void beforeIndexShardPostRecovery(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardPostRecovery(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard post recovery callback", t, indexShard.shardId());
                throw t;
            }
        }
    }


    public void afterIndexShardPostRecovery(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardPostRecovery(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard post recovery callback", t, indexShard.shardId());
                throw t;
            }
        }
    }

    public void afterIndexShardStarted(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardStarted(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard started callback", t, indexShard.shardId());
                throw t;
            }
        }
    }

    public void beforeIndexClosed(IndexService indexService) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexClosed(indexService);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index closed callback", t, indexService.index().name());
                throw t;
            }
        }
    }

    public void beforeIndexDeleted(IndexService indexService) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexDeleted(indexService);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke before index deleted callback", t, indexService.index().name());
                throw t;
            }
        }
    }

    public void afterIndexDeleted(Index index, @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexDeleted(index, indexSettings);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke after index deleted callback", t, index.name());
                throw t;
            }
        }
    }

    public void afterIndexClosed(Index index, @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexClosed(index, indexSettings);
            } catch (Throwable t) {
                logger.warn("[{}] failed to invoke after index closed callback", t, index.name());
                throw t;
            }
        }
    }

    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                       @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard closed callback", t, shardId);
                throw t;
            }
        }
    }

    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard,
                                      @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard closed callback", t, shardId);
                throw t;
            }
        }
    }

    public void beforeIndexShardDeleted(ShardId shardId,
                                       @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.beforeIndexShardDeleted(shardId, indexSettings);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke before shard deleted callback", t, shardId);
                throw t;
            }
        }
    }

    public void afterIndexShardDeleted(ShardId shardId,
                                      @IndexSettings Settings indexSettings) {
        for (Listener listener : listeners) {
            try {
                listener.afterIndexShardDeleted(shardId, indexSettings);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke after shard deleted callback", t, shardId);
                throw t;
            }
        }
    }

    public void indexShardStateChanged(IndexShard indexShard, @Nullable IndexShardState previousState, @Nullable String reason) {
        for (Listener listener : listeners) {
            try {
                listener.indexShardStateChanged(indexShard, previousState, indexShard.state(), reason);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke index shard state changed callback", t, indexShard.shardId());
                throw t;
            }
        }
    }

    public void onShardInactive(IndexShard indexShard) {
        for (Listener listener : listeners) {
            try {
                listener.onShardInactive(indexShard);
            } catch (Throwable t) {
                logger.warn("{} failed to invoke on shard inactive callback", t, indexShard.shardId());
                throw t;
            }
        }
    }
}
