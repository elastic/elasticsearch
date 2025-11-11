/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.flush;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationRequestSplitHelper;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TransportShardFlushAction extends TransportReplicationAction<ShardFlushRequest, ShardFlushRequest, ReplicationResponse> {

    public static final String NAME = FlushAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME);

    private final ProjectResolver projectResolver;

    @Inject
    public TransportShardFlushAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        ProjectResolver projectResolver
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
        this.projectResolver = projectResolver;
        transportService.registerRequestHandler(
            PRE_SYNCED_FLUSH_ACTION_NAME,
            threadPool.executor(ThreadPool.Names.FLUSH),
            PreShardSyncedFlushRequest::new,
            new PreSyncedFlushTransportHandler(indicesService)
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

    // We are here because there was a mismatch between the SplitShardCountSummary in the request
    // and that on the primary shard node. We assume that the request is exactly 1 reshard split behind
    // the current state.
    @Override
    protected Map<ShardId, ShardFlushRequest> splitRequestOnPrimary(ShardFlushRequest request) {
        return ReplicationRequestSplitHelper.splitRequestCommon(
            request,
            projectResolver.getProjectMetadata(clusterService.state()),
            (targetShard, shardCountSummary) ->
                new ShardFlushRequest(request.getRequest(), targetShard, shardCountSummary)
        );

        /*
        ProjectMetadata project = projectResolver.getProjectMetadata(clusterService.state());
        final ShardId sourceShard = request.shardId();
        IndexMetadata indexMetadata = project.getIndexSafe(request.shardId().getIndex());
        SplitShardCountSummary shardCountSummary = SplitShardCountSummary.forIndexing(indexMetadata, sourceShard.getId());
        Map<ShardId, ShardFlushRequest> requestsByShard = new HashMap<>();
        requestsByShard.put(sourceShard, request);
        // Create a request for original source shard and for each target shard.
        // New requests that are to be handled by target shards should contain the
        // latest ShardCountSummary.
        int targetShardId = indexMetadata.getReshardingMetadata().getSplit().targetShard(sourceShard.id());
        ShardId targetShard = new ShardId(request.shardId().getIndex(), targetShardId);
        requestsByShard.put(targetShard, new ShardFlushRequest(request.getRequest(), targetShard, shardCountSummary));
        return requestsByShard;
         */
    }

    @Override
    protected Tuple<ReplicationResponse, Exception> combineSplitResponses(
        ShardFlushRequest originalRequest,
        Map<ShardId, ShardFlushRequest> splitRequests,
        Map<ShardId, Tuple<ReplicationResponse, Exception>> responses
    ) {
        int failed = 0;
        int successful = 0;
        int total = 0;
        List<ReplicationResponse.ShardInfo.Failure> failures = new ArrayList<>();

        // If the action fails on either one of the shards, we return an exception.
        // Case 1: Both source and target shards return a response: Add up total, successful, failures
        // Case 2: Both source and target shards return an exception : return exception
        // Case 3: One shards returns a response, the other returns an exception : return exception
        for (Map.Entry<ShardId, Tuple<ReplicationResponse, Exception>> entry : responses.entrySet()) {
            ShardId shardId = entry.getKey();
            Tuple<ReplicationResponse, Exception> value = entry.getValue();
            Exception exception = value.v2();
            if (exception != null) {
                return new Tuple<>(null, exception);
            } else {
                ReplicationResponse response = value.v1();
                failed += response.getShardInfo().getFailed();
                successful += response.getShardInfo().getSuccessful();
                total += response.getShardInfo().getTotal();
                Collections.addAll(failures, response.getShardInfo().getFailures());
            }
        }
        ReplicationResponse.ShardInfo.Failure[] failureArray = failures.toArray(new ReplicationResponse.ShardInfo.Failure[0]);
        assert failureArray.length == failed;
        ReplicationResponse.ShardInfo shardInfo = ReplicationResponse.ShardInfo.of(total, successful, failureArray);
        ReplicationResponse response = new ReplicationResponse();
        response.setShardInfo(shardInfo);
        return new Tuple<>(response, null);
    }

    @Override
    protected void shardOperationOnReplica(ShardFlushRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        replica.flush(request.getRequest(), listener.map(flushed -> {
            logger.trace("{} flush request executed on replica", replica.shardId());
            return new ReplicaResult();
        }));
    }

    // TODO: Remove this transition in 9.0
    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";

    private static class PreShardSyncedFlushRequest extends AbstractTransportRequest {
        private final ShardId shardId;

        private PreShardSyncedFlushRequest(StreamInput in) throws IOException {
            super(in);
            assert in.getTransportVersion().before(TransportVersions.V_8_0_0) : "received pre_sync request from a new node";
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
