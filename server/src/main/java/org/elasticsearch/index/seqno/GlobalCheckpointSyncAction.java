/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Background global checkpoint sync action initiated when a shard goes inactive. This is needed because while we send the global checkpoint
 * on every replication operation, after the last operation completes the global checkpoint could advance but without a follow-up operation
 * the global checkpoint will never be synced to the replicas.
 */
public class GlobalCheckpointSyncAction extends TransportReplicationAction<
        GlobalCheckpointSyncAction.Request,
        GlobalCheckpointSyncAction.Request,
        ReplicationResponse> {

    public static String ACTION_NAME = "indices:admin/seq_no/global_checkpoint_sync";
    public static ActionType<ReplicationResponse> TYPE = new ActionType<>(ACTION_NAME, ReplicationResponse::new);

    @Inject
    public GlobalCheckpointSyncAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters) {
        super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                Request::new,
                Request::new,
                ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(Request request, IndexShard indexShard,
                                           ActionListener<PrimaryResult<Request, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            maybeSyncTranslog(indexShard);
            return new PrimaryResult<>(request, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            maybeSyncTranslog(replica);
            return new ReplicaResult();
        });
    }

    private void maybeSyncTranslog(final IndexShard indexShard) throws IOException {
        if (indexShard.getTranslogDurability() == Translog.Durability.REQUEST &&
            indexShard.getLastSyncedGlobalCheckpoint() < indexShard.getLastKnownGlobalCheckpoint()) {
            indexShard.sync();
        }
    }

    public static final class Request extends ReplicationRequest<Request> {

        private Request(StreamInput in) throws IOException {
            super(in);
        }

        public Request(final ShardId shardId) {
            super(shardId);
        }

        @Override
        public String toString() {
            return "GlobalCheckpointSyncAction.Request{" +
                    "shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    "}";
        }
    }

}
