/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.truncate;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.refresh.TransportUnpromotableShardRefreshAction;
import org.elasticsearch.action.admin.indices.refresh.UnpromotableShardRefreshRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.BroadcastRequestSplitHelper;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/**
 * Truncates a single shard on the primary and then on each replica.
 * <p>
 * Like the verify-shard actions used when closing an index, this takes <em>all</em> of the shard's operation permits on every copy.
 * On the primary that waits for in-flight writes to finish and holds new ones back; because the primary only releases a write's
 * permit after every in-sync replica acknowledged it, no write with a lower sequence number can still be on its way to a replica when
 * the truncate is replicated, and no write with a higher one can start before the truncate is acknowledged by all of them. That is
 * what lets the truncate be a single point in the sequence number space.
 */
public class TransportShardTruncateAction extends TransportReplicationAction<
    ShardTruncateRequest,
    ShardTruncateRequest,
    ReplicationResponse> {

    public static final String NAME = TruncateIndexAction.NAME + "[s]";
    public static final ActionType<ReplicationResponse> TYPE = new ActionType<>(NAME);

    @Inject
    public TransportShardTruncateAction(
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
            ShardTruncateRequest::new,
            ShardTruncateRequest::new,
            threadPool.executor(ThreadPool.Names.FLUSH),
            // replicas should learn promptly that the truncate is globally persisted so that they can release the segments it dropped
            SyncGlobalCheckpointAfterOperation.AttemptAfterSuccess,
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
        final ShardTruncateRequest request,
        final ActionListener<Releasable> onAcquired
    ) {
        primary.acquireAllPrimaryOperationsPermits(onAcquired, request.timeout());
    }

    @Override
    protected void acquireReplicaOperationPermit(
        final IndexShard replica,
        final ShardTruncateRequest request,
        final ActionListener<Releasable> onAcquired,
        final long primaryTerm,
        final long globalCheckpoint,
        final long maxSeqNoOfUpdatesOrDeletes
    ) {
        replica.acquireAllReplicaOperationsPermits(
            primaryTerm,
            globalCheckpoint,
            maxSeqNoOfUpdatesOrDeletes,
            onAcquired,
            request.timeout()
        );
    }

    @Override
    protected void shardOperationOnPrimary(
        ShardTruncateRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardTruncateRequest, ReplicationResponse>> listener
    ) {
        // Like a write, a truncate needs a shard that can be mutated: a hollow stateless shard is unhollowed first. All permits are
        // already held, which is what the gate expects of a mutable operation that has acquired its permit.
        primary.ensureMutable(
            listener.delegateFailure((l, ignored) -> truncateOnPrimary(request, primary, l)),
            true,
            threadPool.executor(ThreadPool.Names.FLUSH)
        );
    }

    private void truncateOnPrimary(
        ShardTruncateRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<ShardTruncateRequest, ReplicationResponse>> listener
    ) {
        ActionListener.run(listener, l -> {
            final Engine.TruncateResult result = primary.applyTruncateOperationOnPrimary();
            logger.trace("{} truncate executed on primary with seq# [{}]", primary.shardId(), result.getSeqNo());
            // The truncate must be durable before this copy reports it so that the global checkpoint can move past it. Then the
            // refresh makes searches see the empty shard, and tells us which commit the unpromotable copies must catch up to.
            primary.flush(
                new FlushRequest().force(true).waitIfOngoing(true),
                l.delegateFailureAndWrap(
                    (l2, flushed) -> primary.externalRefresh(
                        "truncate",
                        l2.map(
                            refreshResult -> new PrimaryResult<>(
                                request.forReplicas(result.getSeqNo(), refreshResult),
                                new ReplicationResponse()
                            )
                        )
                    )
                )
            );
        });
    }

    @Override
    protected void shardOperationOnReplica(ShardTruncateRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.run(listener, l -> {
            replica.applyTruncateOperationOnReplica(request.seqNo(), replica.getOperationPrimaryTerm());
            logger.trace("{} truncate executed on replica with seq# [{}]", replica.shardId(), request.seqNo());
            replica.refresh("truncate");
            replica.flush(new FlushRequest().force(true).waitIfOngoing(true), l.map(flushed -> new ReplicaResult()));
        });
    }

    @Override
    protected ReplicationOperation.Replicas<ShardTruncateRequest> newReplicasProxy() {
        return new UnpromotableReplicasRefreshProxy();
    }

    /**
     * Unpromotable (search) copies are not part of the replication group, so they are not sent the truncate. They follow the
     * primary through the commits it produces, and, as for a refresh, are told here which commit they have to wait for so that a
     * search after the truncate was acknowledged does not see documents it discarded.
     */
    private class UnpromotableReplicasRefreshProxy extends ReplicasProxy {
        @Override
        public void onPrimaryOperationComplete(
            ShardTruncateRequest replicaRequest,
            IndexShardRoutingTable indexShardRoutingTable,
            ActionListener<Void> listener
        ) {
            final var refreshResult = replicaRequest.primaryRefreshResult;
            transportService.sendRequest(
                transportService.getLocalNode(),
                TransportUnpromotableShardRefreshAction.NAME,
                new UnpromotableShardRefreshRequest(indexShardRoutingTable, refreshResult.primaryTerm(), refreshResult.generation(), false),
                new ActionListenerResponseHandler<>(
                    listener.safeMap(r -> null),
                    in -> ActionResponse.Empty.INSTANCE,
                    threadPool.executor(ThreadPool.Names.REFRESH)
                )
            );
        }
    }

    /**
     * The request was created before a reshard split that the primary has since seen, so it also has to run on the target shards of
     * the split. See {@link TransportReplicationAction#splitRequestOnPrimary}.
     */
    @Override
    protected Map<ShardId, ShardTruncateRequest> splitRequestOnPrimary(ShardTruncateRequest request, ProjectMetadata project) {
        return BroadcastRequestSplitHelper.splitRequest(
            request,
            project,
            (targetShard, shardCountSummary) -> new ShardTruncateRequest(targetShard, shardCountSummary)
        );
    }

    @Override
    protected Tuple<ReplicationResponse, Exception> combineSplitResponses(
        ShardTruncateRequest originalRequest,
        Map<ShardId, ShardTruncateRequest> splitRequests,
        Map<ShardId, Tuple<ReplicationResponse, Exception>> responses
    ) {
        return BroadcastRequestSplitHelper.combineSplitResponses(originalRequest, splitRequests, responses);
    }
}
