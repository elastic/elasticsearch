/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.cluster.IndexRemovalReason;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.snapshots.SnapshotsCommitService;

import java.util.Objects;
import java.util.concurrent.Executor;

/// [IndexEventListener] that manages shard lifecycle on stateless index nodes.
///
/// Registers each shard with the [org.elasticsearch.xpack.stateless.commits.StatelessCommitService] and
/// [org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator] on creation and cleans up those
/// registrations on close or deletion. Also, coordinates snapshot commit release and hollow shard tracking.
class StatelessIndexNodeLifecycleListener implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(StatelessIndexNodeLifecycleListener.class);
    private final StatelessCommitService statelessCommitService;
    private final TranslogReplicator localTranslogReplicator;
    private final SnapshotsCommitService snapshotsCommitService;
    private final Executor commitSuccessExecutor;
    private final HollowShardsService hollowShardsService;

    StatelessIndexNodeLifecycleListener(
        StatelessCommitService statelessCommitService,
        TranslogReplicator localTranslogReplicator,
        SnapshotsCommitService snapshotsCommitService,
        Executor commitSuccessExecutor,
        HollowShardsService hollowShardsService
    ) {
        this.statelessCommitService = Objects.requireNonNull(statelessCommitService);
        this.localTranslogReplicator = Objects.requireNonNull(localTranslogReplicator);
        this.snapshotsCommitService = Objects.requireNonNull(snapshotsCommitService);
        this.commitSuccessExecutor = Objects.requireNonNull(commitSuccessExecutor);
        this.hollowShardsService = Objects.requireNonNull(hollowShardsService);
    }

    @Override
    public void afterIndexShardCreated(IndexShard indexShard) {
        statelessCommitService.register(
            indexShard.shardId(),
            indexShard.getOperationPrimaryTerm(),
            () -> isInitializingNoSearchShards(indexShard),
            () -> indexShard.mapperService().mappingLookup(),
            indexShard::addGlobalCheckpointListener,
            () -> indexShard.withEngine(engine -> {
                if (engine instanceof IndexEngine indexEngine) {
                    indexEngine.syncTranslogReplicator(ActionListener.noop());
                } else {
                    assert false : "Engine is " + engine;
                    throw new IllegalStateException("Engine is " + engine);
                }
                return null;
            })
        );
        localTranslogReplicator.register(
            indexShard.shardId(),
            indexShard.getOperationPrimaryTerm(),
            seqNos -> indexShard.withEngineOrNull(engine -> {
                if (engine instanceof IndexEngine indexEngine) {
                    indexEngine.objectStorePersistedSeqNoConsumer().accept(seqNos);
                    // The local checkpoint is updated as part of the post-replication actions of ReplicationOperation. However, if
                    // a bulk request has a refresh included, the post-replication actions happen after the refresh. And the refresh
                    // may need to wait for the checkpoint to progress in order to send out a new VBCC commit notification. To
                    // break this stalemate, we update the checkpoint as early as here, when the translog has persisted a seqno.
                    // We exclude the initializing state since the replication tracker may not yet be in primary mode and the local
                    // checkpoint is updated as part of recovery. We ignore errors since this is best effort.
                    try {
                        if (indexShard.routingEntry().state() != ShardRoutingState.INITIALIZING) {
                            indexShard.updateLocalCheckpointForShard(
                                indexShard.routingEntry().allocationId().getId(),
                                indexEngine.getPersistedLocalCheckpoint()
                            );
                        }
                    } catch (Exception e) {
                        logger.debug(() -> "Failed to update local checkpoint", e);
                    }
                }
                return null;
            })
        );
        // We are pruning the archive for a given generation, only once we know all search shards are
        // aware of that generation.
        // TODO: In the context of real-time GET, this might be an overkill and in case of misbehaving
        // search shards, this might lead to higher memory consumption on the indexing shards. Depending on
        // how we respond to get requests that are not in the live version map (what generation we send back
        // for the search shard to wait for), it could be safe to trigger the pruning earlier, e.g., once the
        // commit upload is successful.
        statelessCommitService.registerCommitNotificationSuccessListener(indexShard.shardId(), (gen) -> {
            // We dispatch to a generic thread to avoid a transport worker being blocked to get the engine while it's reset
            commitSuccessExecutor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("[{}] failed to notify success of commit notification with generation {}", indexShard.shardId(), gen, e);
                }

                @Override
                protected void doRun() {
                    indexShard.withEngineOrNull(engine -> {
                        if (engine instanceof IndexEngine e) {
                            e.commitSuccess(gen);
                        }
                        return null;
                    });
                }

                @Override
                public String toString() {
                    return "commitSuccess[" + indexShard.shardId() + "]";
                }
            });
        });
    }

    private static boolean isInitializingNoSearchShards(IndexShard shard) {
        final ShardRouting shardRouting = shard.routingEntry();
        return shardRouting.initializing() && shardRouting.recoverySource().getType() != RecoverySource.Type.PEER;
    }

    @Override
    public void beforeIndexRemoved(IndexService indexService, IndexRemovalReason reason) {
        if (reason == IndexRemovalReason.DELETED) {
            statelessCommitService.markIndexDeleting(
                indexService.shardIds().stream().map(id -> new ShardId(indexService.index(), id)).toList()
            );
        }
    }

    @Override
    public void afterIndexShardClosed(ShardId shardId, IndexShard indexShard, Settings indexSettings) {
        if (indexShard != null) {
            statelessCommitService.unregisterCommitNotificationSuccessListener(shardId);
            statelessCommitService.closeShard(shardId);
            // release snapshot commits after shardCommitState is closed
            snapshotsCommitService.releaseCommitsAndRemoveShardAfterShardClosed(shardId);
            hollowShardsService.removeHollowShard(indexShard, "index shard closed");
        }
    }

    @Override
    public void afterIndexShardDeleted(ShardId shardId, Settings indexSettings) {
        statelessCommitService.delete(shardId);
    }

    @Override
    public void onStoreClosed(ShardId shardId) {
        statelessCommitService.unregister(shardId);
        localTranslogReplicator.unregister(shardId);
    }
}
