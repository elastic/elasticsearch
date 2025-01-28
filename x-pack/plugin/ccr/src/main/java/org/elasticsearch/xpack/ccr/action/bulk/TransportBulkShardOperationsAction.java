/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr.action.bulk;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ccr.index.engine.AlreadyProcessedFollowingEngineException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TransportBulkShardOperationsAction extends TransportWriteAction<
    BulkShardOperationsRequest,
    BulkShardOperationsRequest,
    BulkShardOperationsResponse> {

    @Inject
    public TransportBulkShardOperationsAction(
        final Settings settings,
        final TransportService transportService,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final ThreadPool threadPool,
        final ShardStateAction shardStateAction,
        final ActionFilters actionFilters,
        final IndexingPressure indexingPressure,
        final SystemIndices systemIndices
    ) {
        super(
            settings,
            BulkShardOperationsAction.NAME,
            transportService,
            clusterService,
            indicesService,
            threadPool,
            shardStateAction,
            actionFilters,
            BulkShardOperationsRequest::new,
            BulkShardOperationsRequest::new,
            ExecutorSelector.getWriteExecutorForShard(threadPool),
            PrimaryActionExecution.RejectOnOverload,
            indexingPressure,
            systemIndices,
            ReplicaActionExecution.SubjectToCircuitBreaker
        );
    }

    @Override
    protected void doExecute(Task task, BulkShardOperationsRequest request, ActionListener<BulkShardOperationsResponse> listener) {
        // This is executed on the follower coordinator node and we need to mark the bytes.
        Releasable releasable = indexingPressure.markCoordinatingOperationStarted(
            primaryOperationCount(request),
            primaryOperationSize(request),
            false
        );
        ActionListener<BulkShardOperationsResponse> releasingListener = ActionListener.runBefore(listener, releasable::close);
        try {
            super.doExecute(task, request, releasingListener);
        } catch (Exception e) {
            releasingListener.onFailure(e);
        }
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        BulkShardOperationsRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse>> listener
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace("index [{}] on the following primary shard {}", request.getOperations(), primary.routingEntry());
        }
        ActionListener.completeWith(listener, () -> {
            final var result = shardOperationOnPrimary(
                request.shardId(),
                request.getHistoryUUID(),
                request.getOperations(),
                request.getMaxSeqNoOfUpdatesOrDeletes(),
                primary,
                logger,
                postWriteRefresh
            );
            result.replicaRequest().setParentTask(request.getParentTask());
            return result;
        });
    }

    @Override
    protected long primaryOperationSize(BulkShardOperationsRequest request) {
        return request.getOperations().stream().mapToLong(Translog.Operation::estimateSize).sum();
    }

    @Override
    protected int primaryOperationCount(BulkShardOperationsRequest request) {
        return request.getOperations().size();
    }

    public static Translog.Operation rewriteOperationWithPrimaryTerm(Translog.Operation operation, long primaryTerm) {
        final Translog.Operation operationWithPrimaryTerm;
        switch (operation.opType()) {
            case INDEX -> {
                final Translog.Index index = (Translog.Index) operation;
                operationWithPrimaryTerm = new Translog.Index(
                    index.id(),
                    index.seqNo(),
                    primaryTerm,
                    index.version(),
                    index.source(),
                    index.routing(),
                    index.getAutoGeneratedIdTimestamp()
                );
            }
            case DELETE -> {
                final Translog.Delete delete = (Translog.Delete) operation;
                operationWithPrimaryTerm = new Translog.Delete(delete.id(), delete.seqNo(), primaryTerm, delete.version());
            }
            case NO_OP -> {
                final Translog.NoOp noOp = (Translog.NoOp) operation;
                operationWithPrimaryTerm = new Translog.NoOp(noOp.seqNo(), primaryTerm, noOp.reason());
            }
            default -> throw new IllegalStateException("unexpected operation type [" + operation.opType() + "]");
        }
        return operationWithPrimaryTerm;
    }

    // public for testing purposes only
    public static WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> shardOperationOnPrimary(
        final ShardId shardId,
        final String historyUUID,
        final List<Translog.Operation> sourceOperations,
        final long maxSeqNoOfUpdatesOrDeletes,
        final IndexShard primary,
        final Logger logger,
        final PostWriteRefresh postWriteRefresh
    ) throws IOException {
        if (historyUUID.equalsIgnoreCase(primary.getHistoryUUID()) == false) {
            throw new IllegalStateException(
                "unexpected history uuid, expected ["
                    + historyUUID
                    + "], actual ["
                    + primary.getHistoryUUID()
                    + "], shard is likely restored from snapshot or force allocated"
            );
        }

        assert maxSeqNoOfUpdatesOrDeletes >= SequenceNumbers.NO_OPS_PERFORMED : "invalid msu [" + maxSeqNoOfUpdatesOrDeletes + "]";
        primary.advanceMaxSeqNoOfUpdatesOrDeletes(maxSeqNoOfUpdatesOrDeletes);

        final List<Translog.Operation> appliedOperations = new ArrayList<>(sourceOperations.size());
        Translog.Location location = null;
        for (Translog.Operation sourceOp : sourceOperations) {
            final Translog.Operation targetOp = rewriteOperationWithPrimaryTerm(sourceOp, primary.getOperationPrimaryTerm());
            final Engine.Result result = primary.applyTranslogOperation(targetOp, Engine.Operation.Origin.PRIMARY);
            if (result.getResultType() == Engine.Result.Type.SUCCESS) {
                assert result.getSeqNo() == targetOp.seqNo();
                appliedOperations.add(targetOp);
                location = locationToSync(location, result.getTranslogLocation());
            } else {
                if (result.getFailure() instanceof final AlreadyProcessedFollowingEngineException failure) {
                    // The existing operations below the global checkpoint won't be replicated as they were processed
                    // in every replicas already. However, the existing operations above the global checkpoint will be
                    // replicated to replicas but with the existing primary term (not the current primary term) in order
                    // to guarantee the consistency between the primary and replicas, and between translog and Lucene index.
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "operation [{}] was processed before on following primary shard {} with existing term {}",
                            targetOp,
                            primary.routingEntry(),
                            failure.getExistingPrimaryTerm()
                        );
                    }
                    assert failure.getSeqNo() == targetOp.seqNo() : targetOp.seqNo() + " != " + failure.getSeqNo();
                    if (failure.getExistingPrimaryTerm().isPresent()) {
                        appliedOperations.add(rewriteOperationWithPrimaryTerm(sourceOp, failure.getExistingPrimaryTerm().getAsLong()));
                    } else if (targetOp.seqNo() > primary.getLastKnownGlobalCheckpoint()) {
                        assert false
                            : "can't find primary_term for existing op=" + targetOp + " gcp=" + primary.getLastKnownGlobalCheckpoint();
                        throw new IllegalStateException(
                            "can't find primary_term for existing op="
                                + targetOp
                                + " global_checkpoint="
                                + primary.getLastKnownGlobalCheckpoint(),
                            failure
                        );
                    }
                } else {
                    assert false : "Only already-processed error should happen; op=[" + targetOp + "] error=[" + result.getFailure() + "]";
                    throw ExceptionsHelper.convertToElastic(result.getFailure());
                }
            }
        }
        final BulkShardOperationsRequest replicaRequest = new BulkShardOperationsRequest(
            shardId,
            historyUUID,
            appliedOperations,
            maxSeqNoOfUpdatesOrDeletes
        );
        return new WritePrimaryResult<>(replicaRequest, new BulkShardOperationsResponse(), location, primary, logger, postWriteRefresh);
    }

    @Override
    protected void dispatchedShardOperationOnReplica(
        BulkShardOperationsRequest request,
        IndexShard replica,
        ActionListener<ReplicaResult> listener
    ) {
        ActionListener.completeWith(listener, () -> {
            if (logger.isTraceEnabled()) {
                logger.trace("index [{}] on the following replica shard {}", request.getOperations(), replica.routingEntry());
            }
            return shardOperationOnReplica(request, replica, logger);
        });
    }

    @Override
    protected long replicaOperationSize(BulkShardOperationsRequest request) {
        return request.getOperations().stream().mapToLong(Translog.Operation::estimateSize).sum();
    }

    @Override
    protected int replicaOperationCount(BulkShardOperationsRequest request) {
        return request.getOperations().size();
    }

    // public for testing purposes only
    public static WriteReplicaResult<BulkShardOperationsRequest> shardOperationOnReplica(
        final BulkShardOperationsRequest request,
        final IndexShard replica,
        final Logger logger
    ) throws IOException {
        assert replica.getMaxSeqNoOfUpdatesOrDeletes() >= request.getMaxSeqNoOfUpdatesOrDeletes()
            : "mus on replica [" + replica + "] < mus of request [" + request.getMaxSeqNoOfUpdatesOrDeletes() + "]";
        Translog.Location location = null;
        for (final Translog.Operation operation : request.getOperations()) {
            final Engine.Result result = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA);
            if (result.getResultType() != Engine.Result.Type.SUCCESS) {
                assert false : "doc-level failure must not happen on replicas; op[" + operation + "] error[" + result.getFailure() + "]";
                throw ExceptionsHelper.convertToElastic(result.getFailure());
            }
            assert result.getSeqNo() == operation.seqNo();
            location = locationToSync(location, result.getTranslogLocation());
        }
        assert request.getOperations().size() == 0 || location != null;
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    @Override
    protected BulkShardOperationsResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardOperationsResponse(in);
    }

    @Override
    protected void adaptResponse(BulkShardOperationsResponse response, IndexShard indexShard) {
        adaptBulkShardOperationsResponse(response, indexShard);
    }

    public static void adaptBulkShardOperationsResponse(BulkShardOperationsResponse response, IndexShard indexShard) {
        final SeqNoStats seqNoStats = indexShard.seqNoStats();
        // return a fresh global checkpoint after the operations have been replicated for the shard follow task
        response.setGlobalCheckpoint(seqNoStats.getGlobalCheckpoint());
        response.setMaxSeqNo(seqNoStats.getMaxSeqNo());
    }

}
