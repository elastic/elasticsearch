/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

public class TransportShardFlushAction extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportShardFlushAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters
    ) {
        super(
            settings,
            NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ShardFlushRequest::new,
            ShardFlushRequest::new,
            threadPool.executor(ThreadPool.Names.FLUSH),
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
    protected void shardOperationOnPrimary(
        ShardFlushRequest shardRequest,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardFlushRequest, ReplicationResponse>> listener
    ) {
        primary.flush(shardRequest.getRequest(), listener.map(flushed -> {
            logger.trace("{} flush request executed on primary", primary.shardId());
            return new PrimaryResult<>(shardRequest, new ReplicationResponse());
        }));
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        replica.flush(request.getRequest(), listener.map(flushed -> {
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        }));
    }
}
