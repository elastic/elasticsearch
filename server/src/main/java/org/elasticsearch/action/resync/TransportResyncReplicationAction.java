/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.resync;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

public class TransportResyncReplicationAction extends TransportWriteAction<
    ResyncReplicationRequest,
    ResyncReplicationRequest,
    ResyncReplicationResponse> implements PrimaryReplicaSyncer.SyncAction {

    private static final String ACTION_NAME = "internal:index/seq_no/resync";

    @Inject
    public TransportResyncReplicationAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices,
        ProjectResolver projectResolver
    ) {
        super(
            settings,
            ACTION_NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            ResyncReplicationRequest::new,
            ResyncReplicationRequest::new,
            ExecutorSelector.getWriteExecutorForShard(threadPool),
            PrimaryActionExecution.Force, /* we should never reject resync because of thread pool capacity on primary */
            indexingPressure,
            systemIndices,
            projectResolver,
            ReplicaActionExecution.SubjectToCircuitBreaker
        );
    }

    @Override
    protected void doExecute(Task parentTask, ResyncReplicationRequest request, ActionListener<ResyncReplicationResponse> listener) {
        assert false : "use TransportResyncReplicationAction#sync";
    }

    @Override
    protected ResyncReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ResyncReplicationResponse(in);
    }

    @Override
    protected ReplicationOperation.Replicas<ResyncReplicationRequest> newReplicasProxy() {
        return new ResyncActionReplicasProxy();
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        // resync should never be blocked because it's an internal action
        return null;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        // resync should never be blocked because it's an internal action
        return null;
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        ResyncReplicationRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse>> listener
    ) {
        ActionListener.completeWith(
            listener,
            () -> new WritePrimaryResult<>(
                performOnPrimary(request),
                new ResyncReplicationResponse(),
                null,
                primary,
                logger,
                postWriteRefresh
            )
        );
    }

    @Override
    protected long primaryOperationSize(ResyncReplicationRequest request) {
        return Stream.of(request.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
    }

    @Override
    protected int primaryOperationCount(ResyncReplicationRequest request) {
        return request.getOperations().length;
    }

    @Override
    protected long primaryLargestOperationSize(ResyncReplicationRequest request) {
        return Stream.of(request.getOperations()).mapToLong(Translog.Operation::estimateSize).max().orElse(0);
    }

    public static ResyncReplicationRequest performOnPrimary(ResyncReplicationRequest request) {
        return request;
    }

    @Override
    protected void dispatchedShardOperationOnReplica(
        ResyncReplicationRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            Translog.Location location = performOnReplica(request, replica);
            return new WriteReplicaResult<>(request, location, null, replica, logger);
        });
    }

    @Override
    protected long replicaOperationSize(ResyncReplicationRequest request) {
        return Stream.of(request.getOperations()).mapToLong(Translog.Operation::estimateSize).sum();
    }

    @Override
    protected int replicaOperationCount(ResyncReplicationRequest request) {
        return request.getOperations().length;
    }

    public static Translog.Location performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        /*
         * Operations received from resync do not have auto_id_timestamp individually, we need to bootstrap this max_seen_timestamp
         * (at least the highest timestamp from any of these operations) to make sure that we will disable optimization for the same
         * append-only requests with timestamp (sources of these operations) that are replicated; otherwise we may have duplicates.
         */
        replica.updateMaxUnsafeAutoIdTimestamp(request.getMaxSeenAutoIdTimestampOnPrimary());
        for (Translog.Operation operation : request.getOperations()) {
            final Engine.Result operationResult = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA);
            if (operationResult.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                throw new TransportReplicationAction.RetryOnReplicaException(
                    replica.shardId(),
                    "Mappings are not available on the replica yet, triggered update: " + operationResult.getRequiredMappingUpdate()
                );
            }
            location = syncOperationResultOrThrow(operationResult, location);
        }
        if (request.getTrimAboveSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO) {
            replica.trimOperationOfPreviousPrimaryTerms(request.getTrimAboveSeqNo());
        }
        return location;
    }

    @Override
    public void sync(
        ResyncReplicationRequest request,
        Task parentTask,
        String primaryAllocationId,
        long primaryTerm,
        ActionListener<ResyncReplicationResponse> listener
    ) {
        // skip reroute phase
        transportService.sendChildRequest(
            clusterService.localNode(),
            transportPrimaryAction,
            new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
            parentTask,
            transportOptions,
            new ActionListenerResponseHandler<>(
                listener,
                TransportResyncReplicationAction.this::newResponseInstance,
                TransportResponseHandler.TRANSPORT_WORKER
            ) {

                @Override
                public void handleResponse(ResyncReplicationResponse response) {
                    final ReplicationResponse.ShardInfo.Failure[] failures = response.getShardInfo().getFailures();
                    // noinspection ForLoopReplaceableByForEach
                    for (int i = 0; i < failures.length; i++) {
                        final ReplicationResponse.ShardInfo.Failure f = failures[i];
                        logger.info(
                            () -> format("%s primary-replica resync to replica on node [%s] failed", f.fullShardId(), f.nodeId()),
                            f.getCause()
                        );
                    }
                    listener.onResponse(response);
                }
            }
        );
    }

    /**
     * A proxy for primary-replica resync operations which are performed on replicas when a new primary is promoted.
     * Replica shards fail to execute resync operations will be failed but won't be marked as stale.
     * This avoids marking shards as stale during cluster restart but enforces primary-replica resync mandatory.
     */
    class ResyncActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(
            ShardRouting replica,
            long primaryTerm,
            String message,
            Exception exception,
            ActionListener<Void> listener
        ) {
            shardStateAction.remoteShardFailed(
                replica.shardId(),
                replica.allocationId().getId(),
                primaryTerm,
                false,
                message,
                exception,
                listener
            );
        }
    }
}
