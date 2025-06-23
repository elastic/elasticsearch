/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.indices.readonly;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Action used to verify whether shards have properly applied a given index block,
 * and are no longer executing any operations in violation of that block. This action
 * requests all operation permits of the shard in order to wait for all write operations
 * to complete.
 */
public class TransportVerifyShardIndexBlockAction extends TransportReplicationAction<
    TransportVerifyShardIndexBlockAction.ShardRequest,
    TransportVerifyShardIndexBlockAction.ShardRequest,
    ReplicationResponse> {

    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(TransportAddIndexBlockAction.TYPE.name() + "[s]");

    @Inject
    public TransportVerifyShardIndexBlockAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction stateAction,
        final ActionFilters actionFilters
    ) {
        super(
            settings,
            TYPE.name(),
            transportService,
            clusterService,
            indicesService,
            threadPool,
            stateAction,
            actionFilters,
            ShardRequest::new,
            ShardRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT),
            SyncGlobalCheckpointAfterOperation.DoNotSync,
            PrimaryActionExecution.RejectOnOverload,
            ReplicaActionExecution.SubjectToCircuitBreaker
        );
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void acquirePrimaryOperationPermit(
        final IndexShard primary,
        final ShardRequest request,
        final ActionListener<Releasable> onAcquired
    ) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(
        final IndexShard replica,
        final ShardRequest request,
        final ActionListener<Releasable> onAcquired,
        final long primaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdateOrDeletes
    ) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    @Override
    protected void shardOperationOnPrimary(
        final ShardRequest shardRequest,
        final IndexShard primary,
        ActionListener<PrimaryResult<ShardRequest, ReplicationResponse>> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, primary);
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardRequest shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            executeShardOperation(shardRequest, replica);
            return new ReplicaResult();
        });
    }

    private void executeShardOperation(final ShardRequest request, final IndexShard indexShard) throws IOException {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.getActiveOperationsCount() != IndexShard.OPERATIONS_BLOCKED) {
            throw new IllegalStateException(
                "index shard " + shardId + " is not blocking all operations while waiting for block " + request.clusterBlock()
            );
        }

        final ClusterState clusterState = clusterService.state();
        final ClusterBlocks clusterBlocks = clusterState.blocks();
        final ProjectId projectId = clusterState.metadata().projectFor(shardId.getIndex()).id();
        if (clusterBlocks.hasIndexBlock(projectId, shardId.getIndexName(), request.clusterBlock()) == false) {
            throw new IllegalStateException("index shard " + shardId + " has not applied block " + request.clusterBlock());
        }

        // same pattern as in TransportVerifyShardBeforeCloseAction, but could also flush in phase1.
        if (request.phase1()) {
            indexShard.sync();
        } else {
            if (request.clusterBlock().contains(ClusterBlockLevel.WRITE)) {
                indexShard.flush(new FlushRequest().force(true).waitIfOngoing(true));
            }
        }
    }

    @Override
    protected ReplicationOperation.Replicas<ShardRequest> newReplicasProxy() {
        return new VerifyShardReadOnlyActionReplicasProxy();
    }

    /**
     * A {@link ReplicasProxy} that marks as stale the shards that are unavailable during the verification
     * and the flush of the shard. This is done to ensure that such shards won't be later promoted as primary
     * or reopened in an unverified state with potential non flushed translog operations.
     */
    class VerifyShardReadOnlyActionReplicasProxy extends ReplicasProxy {
        @Override
        public void markShardCopyAsStaleIfNeeded(
            final ShardId shardId,
            final String allocationId,
            final long primaryTerm,
            final ActionListener<Void> listener
        ) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }

    public static final class ShardRequest extends ReplicationRequest<ShardRequest> {

        private final ClusterBlock clusterBlock;
        private final boolean phase1;

        ShardRequest(StreamInput in) throws IOException {
            super(in);
            clusterBlock = new ClusterBlock(in);
            if (in.getTransportVersion().onOrAfter(TransportVersions.ADD_INDEX_BLOCK_TWO_PHASE)) {
                phase1 = in.readBoolean();
            } else {
                phase1 = true; // does not matter, not verified anyway
            }
        }

        public ShardRequest(final ShardId shardId, final ClusterBlock clusterBlock, boolean phase1, final TaskId parentTaskId) {
            super(shardId);
            this.clusterBlock = Objects.requireNonNull(clusterBlock);
            this.phase1 = phase1;
            setParentTask(parentTaskId);
        }

        @Override
        public String toString() {
            return "verify shard " + shardId + " before block with " + clusterBlock + " phase1=" + phase1;
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterBlock.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.ADD_INDEX_BLOCK_TWO_PHASE)) {
                out.writeBoolean(phase1);
            }
        }

        public ClusterBlock clusterBlock() {
            return clusterBlock;
        }

        public boolean phase1() {
            return phase1;
        }
    }
}
