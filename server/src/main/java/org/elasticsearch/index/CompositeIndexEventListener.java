/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndicesClusterStateService.AllocatedIndices.IndexRemovalReason;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

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
                logger.warn(() -> "[" + indexShard.shardId().getId() + "] failed to invoke shard touring changed callback", e);
            }
        }
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardCreated(indexShard);
            } catch (Exception e) {
                logger.warn(() -> "[" + indexShard.shardId().getId() + "] failed to invoke after shard created callback", e);
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
                logger.warn(() -> "[" + indexShard.shardId().getId() + "] failed to invoke after shard started callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> "[" + shardId.getId() + "] failed to invoke before shard closed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardClosed(shardId, indexShard, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> "[" + shardId.getId() + "] failed to invoke after shard closed callback", e);
                throw e;
            }
        }
    }

    @Override
    public void indexShardStateChanged(
        IndexShard indexShard,
        @Nullable IndexShardState previousState,
        IndexShardState currentState,
        @Nullable String reason
    ) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.indexShardStateChanged(indexShard, previousState, indexShard.state(), reason);
            } catch (Exception e) {
                logger.warn(() -> format("[%s] failed to invoke index shard state changed callback", indexShard.shardId().getId()), e);
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
    public void beforeIndexShardCreated(ShardRouting shardRouting, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardCreated(shardRouting, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> "[" + shardRouting + "] failed to invoke before shard created callback", e);
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
    public void beforeIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.beforeIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> "[" + shardId.getId() + "] failed to invoke before shard deleted callback", e);
                throw e;
            }
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterIndexShardDeleted(shardId, indexSettings);
            } catch (Exception e) {
                logger.warn(() -> "[" + shardId.getId() + "] failed to invoke after shard deleted callback", e);
                throw e;
            }
        }
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        for (IndexEventListener listener : listeners) {
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
        for (IndexEventListener listener : listeners) {
            try {
                listener.onStoreClosed(shardId);
            } catch (Exception e) {
                logger.warn("failed to invoke on store closed", e);
                throw e;
            }
        }
    }

    private void iterateBeforeIndexShardRecovery(
        final IndexShard indexShard,
        final IndexSettings indexSettings,
        final Iterator<IndexEventListener> iterator,
        final ActionListener<Void> outerListener
    ) {
        while (iterator.hasNext()) {
            final var nextListener = iterator.next();
            final var future = new ListenableFuture<Void>();
            try {
                nextListener.beforeIndexShardRecovery(indexShard, indexSettings, future);
                if (future.isDone()) {
                    // common case, not actually async, so just check for an exception and continue on the same thread
                    future.result();
                    continue;
                }
            } catch (Exception e) {
                outerListener.onFailure(e);
                return;
            }

            // future was not completed straight away, but might be done by now, so continue on a fresh thread to avoid stack overflow
            future.addListener(
                outerListener.delegateFailure(
                    (delegate, v) -> indexShard.getThreadPool()
                        .executor(ThreadPool.Names.GENERIC)
                        .execute(
                            ActionRunnable.wrap(delegate, l -> iterateBeforeIndexShardRecovery(indexShard, indexSettings, iterator, l))
                        )
                )
            );
            return;
        }

        outerListener.onResponse(null);
    }

    @Override
    public void beforeIndexShardRecovery(
        final IndexShard indexShard,
        final IndexSettings indexSettings,
        final ActionListener<Void> outerListener
    ) {
        iterateBeforeIndexShardRecovery(indexShard, indexSettings, listeners.iterator(), outerListener.delegateResponse((l, e) -> {
            logger.warn(() -> format("failed to invoke the listener before the shard recovery starts for %s", indexShard.shardId()), e);
            l.onFailure(e);
        }));
    }

    @Override
    public void afterFilesRestoredFromRepository(IndexShard indexShard) {
        for (IndexEventListener listener : listeners) {
            try {
                listener.afterFilesRestoredFromRepository(indexShard);
            } catch (Exception e) {
                logger.warn(() -> "[" + indexShard.shardId() + "] failed to invoke after files restored from repository", e);
                throw e;
            }
        }
    }
}
