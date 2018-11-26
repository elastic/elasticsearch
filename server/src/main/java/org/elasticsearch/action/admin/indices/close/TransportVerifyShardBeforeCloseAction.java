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
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportVerifyShardBeforeCloseAction extends TransportReplicationAction<
    TransportVerifyShardBeforeCloseAction.ShardCloseRequest, TransportVerifyShardBeforeCloseAction.ShardCloseRequest, ReplicationResponse> {

    public static final String NAME = CloseIndexAction.NAME + "[s]";
    private static final ClusterBlock EXPECTED_BLOCK = MetaDataIndexStateService.INDEX_CLOSED_BLOCK;

    @Inject
    public TransportVerifyShardBeforeCloseAction(final Settings settings, final TransportService transportService,
                                                 final ClusterService clusterService, final IndicesService indicesService,
                                                 final ThreadPool threadPool, final ShardStateAction stateAction,
                                                 final ActionFilters actionFilters, final IndexNameExpressionResolver resolver) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, stateAction, actionFilters, resolver,
            ShardCloseRequest::new, ShardCloseRequest::new, ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance() {
        return new ReplicationResponse();
    }

    @Override
    protected void acquirePrimaryOperationPermit(final IndexShard primary,
                                                 final ShardCloseRequest request,
                                                 final ActionListener<Releasable> onAcquired) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(final IndexShard replica,
                                                 final ShardCloseRequest request,
                                                 final ActionListener<Releasable> onAcquired,
                                                 final long primaryTerm,
                                                 final long globalCheckpoint,
                                                 final long maxSeqNoOfUpdateOrDeletes) {
        replica.acquireAllReplicaOperationsPermits(primaryTerm, globalCheckpoint, maxSeqNoOfUpdateOrDeletes, onAcquired, request.timeout());
    }

    @Override
    protected PrimaryResult<ShardCloseRequest, ReplicationResponse> shardOperationOnPrimary(final ShardCloseRequest shardRequest,
                                                                                            final IndexShard primary) throws Exception {
        executeShardOperation(primary);
        return new PrimaryResult<>(shardRequest, new ReplicationResponse());
    }

    @Override
    protected ReplicaResult shardOperationOnReplica(final ShardCloseRequest shardRequest, final IndexShard replica) throws Exception {
        executeShardOperation(replica);
        return new ReplicaResult();
    }

    private void executeShardOperation(final IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        if (indexShard.getActiveOperationsCount() != 0) {
            throw new IllegalStateException("On-going operations in progress while checking index shard " + shardId + " before closing");
        }

        final ClusterBlocks clusterBlocks = clusterService.state().blocks();
        if (clusterBlocks.hasIndexBlock(shardId.getIndexName(), EXPECTED_BLOCK) == false) {
            throw new IllegalStateException("Index shard " + shardId + " must be blocked by " + EXPECTED_BLOCK + " before closing");
        }

        final long maxSeqNo = indexShard.seqNoStats().getMaxSeqNo();
        if (indexShard.getGlobalCheckpoint() != maxSeqNo) {
            throw new IllegalStateException("Global checkpoint [" + indexShard.getGlobalCheckpoint()
                + "] mismatches maximum sequence number [" + maxSeqNo + "] on index shard " + shardId);
        }
        indexShard.flush(new FlushRequest());
        logger.debug("{} shard is ready for closing", shardId);
    }

    public static class ShardCloseRequest extends ReplicationRequest<ShardCloseRequest> {

        ShardCloseRequest(){
        }

        public ShardCloseRequest(final ShardId shardId) {
            super(shardId);
        }

        @Override
        public String toString() {
            return "close shard {" + shardId + "}";
        }
    }
}
