/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardFlushAction
        extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME, ReplicationResponse::new);

    @Inject
    public TransportShardFlushAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                     ActionFilters actionFilters) {
        super(settings, NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
            actionFilters, ShardFlushRequest::new, ShardFlushRequest::new, ThreadPool.Names.FLUSH);
        transportService.registerRequestHandler(PRE_SYNCED_FLUSH_ACTION_NAME,
            ThreadPool.Names.FLUSH, PreShardSyncedFlushRequest::new, new PreSyncedFlushTransportHandler(indicesService));
    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

    @Override
    protected void shardOperationOnPrimary(ShardFlushRequest shardRequest, IndexShard primary,
            ActionListener<PrimaryResult<ShardFlushRequest, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            primary.flush(shardRequest.getRequest());
            logger.trace("{} flush request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            replica.flush(request.getRequest());
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        });
    }

    // TODO: Remove this transition in 9.0
    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";

    private static class PreShardSyncedFlushRequest extends TransportRequest {
        private final ShardId shardId;

        private PreShardSyncedFlushRequest(StreamInput in) throws IOException {
            super(in);
            assert in.getVersion().before(Version.V_8_0_0) : "received pre_sync request from a new node";
            this.shardId = new ShardId(in);
        }

        @Override
        public String toString() {
            return "PreShardSyncedFlushRequest{" + "shardId=" + shardId + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            assert false : "must not send pre_sync request from a new node";
            throw new UnsupportedOperationException("");
        }
    }

    private static final class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreShardSyncedFlushRequest> {
        private final IndicesService indicesService;

        PreSyncedFlushTransportHandler(IndicesService indicesService) {
            this.indicesService = indicesService;
        }

        @Override
        public void messageReceived(PreShardSyncedFlushRequest request, TransportChannel channel, Task task) {
            IndexShard indexShard = indicesService.indexServiceSafe(request.shardId.getIndex()).getShard(request.shardId.id());
            indexShard.flush(new FlushRequest().force(false).waitIfOngoing(true));
            throw new UnsupportedOperationException("Synced flush was removed and a normal flush was performed instead.");
        }
    }
}
