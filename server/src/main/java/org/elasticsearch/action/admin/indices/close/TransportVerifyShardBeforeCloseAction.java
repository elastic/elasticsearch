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
package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Consumer;

public class TransportVerifyShardBeforeCloseAction extends TransportReplicationAction<
    TransportVerifyShardBeforeCloseAction.ShardRequest, TransportVerifyShardBeforeCloseAction.ShardRequest, ReplicationResponse> {

    public static final String NAME = CloseIndexAction.NAME + "[s]";

    @Inject
    public TransportVerifyShardBeforeCloseAction(final Settings settings, final TransportService transportService,
                                                 final ClusterService clusterService, final IndicesService indicesService,
                                                 final ThreadPool threadPool, final ShardStateAction stateAction,
                                                 final ActionFilters actionFilters, final IndexNameExpressionResolver resolver) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, stateAction, actionFilters, resolver,
            ShardRequest::new, ShardRequest::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

    @Override
    protected void acquirePrimaryOperationPermit(final IndexShard primary,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(final IndexShard replica,
                                                 final ShardRequest request,
                                                 final ActionListener<Releasable> onAcquired,
                                                 final long primaryTerm,
                                                 final long globalCheckpoint,
                                                 final long maxSeqNoOfUpdateOrDeletes) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    @Override
    protected PrimaryResult<ShardRequest, ReplicationResponse> shardOperationOnPrimary(final ShardRequest shardRequest,
                                                                                       final IndexShard primary) throws Exception {
        executeShardOperation(shardRequest, primary);
        return new PrimaryResult<>(shardRequest, new ReplicationResponse());
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(final ShardRequest shardRequest, final IndexShard replica) throws Exception {
        executeShardOperation(shardRequest, replica);
        return new ReplicaResult();
    }

    private void executeShardOperation(final ShardRequest request, final IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.getActiveOperationsCount() != 0) {
            throw new IllegalStateException("On-going operations in progress while checking index shard " + shardId + " before closing");
        }

        final ClusterBlocks clusterBlocks = clusterService.state().blocks();
        if (clusterBlocks.hasIndexBlock(shardId.getIndexName(), request.clusterBlock()) == false) {
            throw new IllegalStateException("Index shard " + shardId + " must be blocked by " + request.clusterBlock() + " before closing");
        }

        final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
        if (indexShard.getGlobalCheckpoint() != maxSeqNo) {
            throw new IllegalStateException("Global checkpoint [" + indexShard.getGlobalCheckpoint()
                + "] mismatches maximum sequence number [" + maxSeqNo + "] on index shard " + shardId);
        }
        indexShard.flush(new FlushRequest());
        logger.debug("{} shard is ready for closing", shardId);
    }

    @Override
    protected ReplicationOperation.Replicas<ShardRequest> newReplicasProxy(final long primaryTerm) {
        return new VerifyShardBeforeCloseActionReplicasProxy(primaryTerm);
    }

    /**
     * A {@link ReplicasProxy} that marks as stale the shards that are unavailable during the verification
     * and the flush of the shard. This is done to ensure that such shards won't be later promoted as primary
     * or reopened in an unverified state with potential non flushed translog operations.
     */
    class VerifyShardBeforeCloseActionReplicasProxy extends ReplicasProxy {

        VerifyShardBeforeCloseActionReplicasProxy(final long primaryTerm) {
            super(primaryTerm);
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(final ShardId shardId, final String allocationId, final Runnable onSuccess,
                                                 final Consumer<Exception> onPrimaryDemoted, final Consumer<Exception> onIgnoredFailure) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null,
                createShardActionListener(onSuccess, onPrimaryDemoted, onIgnoredFailure));
        }
    }

    public static class ShardRequest extends ReplicationRequest<ShardRequest> {

        private ClusterBlock clusterBlock;

        ShardRequest(){
        }

        public ShardRequest(final ShardId shardId, final ClusterBlock clusterBlock, final TaskId parentTaskId) {
            super(shardId);
            this.clusterBlock = Objects.requireNonNull(clusterBlock);
            setParentTask(parentTaskId);
        }

        @Override
        public String toString() {
            return "verify shard " + shardId + " before close with block " + clusterBlock;
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            clusterBlock = ClusterBlock.readClusterBlock(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterBlock.writeTo(out);
        }

        public ClusterBlock clusterBlock() {
            return clusterBlock;
        }
    }
}
