/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.ExecutorSelector;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

import static org.elasticsearch.core.Strings.format;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";
    public static final ActionType<BulkShardResponse> TYPE = new ActionType<>(ACTION_NAME, BulkShardResponse::new);

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final Consumer<Runnable> postWriteAction;

    @Inject
    public TransportShardBulkAction(
        Settings settings,
        TransportService transportService,
        ClusterService clusterService,
        IndicesService indicesService,
        ThreadPool threadPool,
        ShardStateAction shardStateAction,
        MappingUpdatedAction mappingUpdatedAction,
        UpdateHelper updateHelper,
        ActionFilters actionFilters,
        IndexingPressure indexingPressure,
        SystemIndices systemIndices
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
            BulkShardRequest::new,
            BulkShardRequest::new,
            ExecutorSelector::getWriteExecutorForShard,
            false,
            indexingPressure,
            systemIndices
        );
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.postWriteAction = WriteAckDelay.create(settings, threadPool);
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return BulkAction.INSTANCE.transportOptions();
    }

    @Override
    protected BulkShardResponse newResponseInstance(StreamInput in) throws IOException {
        return new BulkShardResponse(in);
    }

    @Override
    protected void dispatchedShardOperationOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener
    ) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis, (update, shardId, mappingListener) -> {
            assert update != null;
            assert shardId != null;
            mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), update, mappingListener);
        }, mappingUpdateListener -> observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                mappingUpdateListener.onResponse(null);
            }

            @Override
            public void onClusterServiceClose() {
                mappingUpdateListener.onFailure(new NodeClosedException(clusterService.localNode()));
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                mappingUpdateListener.onFailure(new MapperException("timed out while waiting for a dynamic mapping update"));
            }
        }), listener, threadPool, executor(primary), postWriteRefresh, postWriteAction);
    }

    @Override
    protected long primaryOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    @Override
    protected int primaryOperationCount(BulkShardRequest request) {
        return request.items().length;
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        ThreadPool threadPool,
        String executorName
    ) {
        performOnPrimary(
            request,
            primary,
            updateHelper,
            nowInMillisSupplier,
            mappingUpdater,
            waitForMappingUpdate,
            listener,
            threadPool,
            executorName,
            null,
            null
        );
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<PrimaryResult<BulkShardRequest, BulkShardResponse>> listener,
        ThreadPool threadPool,
        String executorName,
        @Nullable PostWriteRefresh postWriteRefresh,
        @Nullable Consumer<Runnable> postWriteAction
    ) {
        new ActionRunnable<>(listener) {

            private final Executor executor = threadPool.executor(executorName);

            private final BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);

            final long startBulkTime = System.nanoTime();

            @Override
            protected void doRun() throws Exception {
                while (context.hasMoreOperationsToExecute()) {
                    if (executeBulkItemRequest(
                        context,
                        updateHelper,
                        nowInMillisSupplier,
                        mappingUpdater,
                        waitForMappingUpdate,
                        ActionListener.wrap(v -> executor.execute(this), this::onRejection)
                    ) == false) {
                        // We are waiting for a mapping update on another thread, that will invoke this action again once its done
                        // so we just break out here.
                        return;
                    }
                    assert context.isInitial(); // either completed and moved to next or reset
                }
                primary.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
                // We're done, there's no more operations to execute so we resolve the wrapped listener
                finishRequest();
            }

            @Override
            public void onRejection(Exception e) {
                // We must finish the outstanding request. Finishing the outstanding request can include
                // refreshing and fsyncing. Therefore, we must force execution on the WRITE thread.
                executor.execute(new ActionRunnable<>(listener) {

                    @Override
                    protected void doRun() {
                        // Fail all operations after a bulk rejection hit an action that waited for a mapping update and finish the request
                        while (context.hasMoreOperationsToExecute()) {
                            context.setRequestToExecute(context.getCurrent());
                            final DocWriteRequest<?> docWriteRequest = context.getRequestToExecute();
                            onComplete(
                                exceptionToResult(
                                    e,
                                    primary,
                                    docWriteRequest.opType() == DocWriteRequest.OpType.DELETE,
                                    docWriteRequest.version(),
                                    docWriteRequest.id()
                                ),
                                context,
                                null
                            );
                        }
                        finishRequest();
                    }

                    @Override
                    public boolean isForceExecution() {
                        return true;
                    }
                });
            }

            private void finishRequest() {
                ActionListener.completeWith(
                    listener,
                    () -> new WritePrimaryResult<>(
                        context.getBulkShardRequest(),
                        context.buildShardResponse(),
                        context.getLocationToSync(),
                        context.getPrimary(),
                        logger,
                        postWriteRefresh,
                        postWriteAction
                    )
                );
            }
        }.run();
    }

    /**
     * Executes bulk item requests and handles request execution exceptions.
     * @return {@code true} if request completed on this thread and the listener was invoked, {@code false} if the request triggered
     *                      a mapping update that will finish and invoke the listener on a different thread
     */
    static boolean executeBulkItemRequest(
        BulkPrimaryExecutionContext context,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        Consumer<ActionListener<Void>> waitForMappingUpdate,
        ActionListener<Void> itemDoneListener
    ) throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        // Translate update requests into index or delete requests which can be executed directly
        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result = new Engine.IndexResult(failure, updateRequest.version(), updateRequest.id());
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            if (updateResult.getResponseResult() == DocWriteResponse.Result.NOOP) {
                context.markOperationAsNoOp(updateResult.action());
                context.markAsCompleted(context.getExecutionResult());
                return true;
            }
            context.setRequestToExecute(updateResult.action());
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        final IndexShard primary = context.getPrimary();
        final long version = context.getRequestToExecute().version();
        final boolean isDelete = context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE;
        final Engine.Result result;
        if (isDelete) {
            final DeleteRequest request = context.getRequestToExecute();
            result = primary.applyDeleteOperationOnPrimary(
                version,
                request.id(),
                request.versionType(),
                request.ifSeqNo(),
                request.ifPrimaryTerm()
            );
        } else {
            final IndexRequest request = context.getRequestToExecute();
            final SourceToParse sourceToParse = new SourceToParse(
                request.id(),
                request.source(),
                request.getContentType(),
                request.routing(),
                request.getDynamicTemplates()
            );
            result = primary.applyIndexOperationOnPrimary(
                version,
                request.versionType(),
                sourceToParse,
                request.ifSeqNo(),
                request.ifPrimaryTerm(),
                request.getAutoGeneratedTimestamp(),
                request.isRetry()
            );
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {

            try {
                primary.mapperService()
                    .merge(
                        MapperService.SINGLE_MAPPING_NAME,
                        new CompressedXContent(result.getRequiredMappingUpdate()),
                        MapperService.MergeReason.MAPPING_UPDATE_PREFLIGHT
                    );
            } catch (Exception e) {
                logger.info(() -> format("%s mapping update rejected by primary", primary.shardId()), e);
                assert result.getId() != null;
                onComplete(exceptionToResult(e, primary, isDelete, version, result.getId()), context, updateResult);
                return true;
            }

            mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), primary.shardId(), new ActionListener<>() {
                @Override
                public void onResponse(Void v) {
                    context.markAsRequiringMappingUpdate();
                    waitForMappingUpdate.accept(ActionListener.runAfter(new ActionListener<>() {
                        @Override
                        public void onResponse(Void v) {
                            assert context.requiresWaitingForMappingUpdate();
                            context.resetForMappingUpdateRetry();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            context.failOnMappingUpdate(e);
                        }
                    }, () -> itemDoneListener.onResponse(null)));
                }

                @Override
                public void onFailure(Exception e) {
                    onComplete(exceptionToResult(e, primary, isDelete, version, result.getId()), context, updateResult);
                    // Requesting mapping update failed, so we don't have to wait for a cluster state update
                    assert context.isInitial();
                    itemDoneListener.onResponse(null);
                }
            });
            return false;
        } else {
            onComplete(result, context, updateResult);
        }
        return true;
    }

    private static Engine.Result exceptionToResult(Exception e, IndexShard primary, boolean isDelete, long version, String id) {
        assert id != null;
        return isDelete ? primary.getFailedDeleteResult(e, version, id) : primary.getFailedIndexResult(e, version, id);
    }

    private static void onComplete(Engine.Result r, BulkPrimaryExecutionContext context, UpdateHelper.Result updateResult) {
        context.markOperationAsExecuted(r);
        final DocWriteRequest<?> docWriteRequest = context.getCurrent();
        final DocWriteRequest.OpType opType = docWriteRequest.opType();
        final boolean isUpdate = opType == DocWriteRequest.OpType.UPDATE;
        final BulkItemResponse executionResult = context.getExecutionResult();
        final boolean isFailed = executionResult.isFailed();
        if (isUpdate
            && isFailed
            && isConflictException(executionResult.getFailure().getCause())
            && context.getUpdateRetryCounter() < ((UpdateRequest) docWriteRequest).retryOnConflict()) {
            context.resetForUpdateRetry();
            return;
        }
        final BulkItemResponse response;
        if (isUpdate) {
            response = processUpdateResponse((UpdateRequest) docWriteRequest, context.getConcreteIndex(), executionResult, updateResult);
        } else {
            if (isFailed) {
                final Exception failure = executionResult.getFailure().getCause();
                Level level;
                if (TransportShardBulkAction.isConflictException(failure)) {
                    level = Level.TRACE;
                } else {
                    level = Level.DEBUG;
                }
                logger.log(
                    level,
                    () -> Strings.format(
                        "%s failed to execute bulk item (%s) %s",
                        context.getPrimary().shardId(),
                        opType.getLowercase(),
                        docWriteRequest
                    ),
                    failure
                );

            }
            response = executionResult;
        }
        context.markAsCompleted(response);
        assert context.isInitial();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    private static BulkItemResponse processUpdateResponse(
        final UpdateRequest updateRequest,
        final String concreteIndex,
        BulkItemResponse operationResponse,
        final UpdateHelper.Result translate
    ) {
        final BulkItemResponse response;
        if (operationResponse.isFailed()) {
            response = BulkItemResponse.failure(
                operationResponse.getItemId(),
                DocWriteRequest.OpType.UPDATE,
                operationResponse.getFailure()
            );
        } else {
            final DocWriteResponse.Result translatedResult = translate.getResponseResult();
            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
                final IndexRequest updateIndexRequest = translate.action();
                final IndexResponse indexResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    indexResponse.getShardInfo(),
                    indexResponse.getShardId(),
                    indexResponse.getId(),
                    indexResponse.getSeqNo(),
                    indexResponse.getPrimaryTerm(),
                    indexResponse.getVersion(),
                    indexResponse.getResult()
                );

                if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                    final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                    final Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(
                        indexSourceAsBytes,
                        true,
                        updateIndexRequest.getContentType()
                    );
                    updateResponse.setGetResult(
                        UpdateHelper.extractGetResult(
                            updateRequest,
                            concreteIndex,
                            indexResponse.getSeqNo(),
                            indexResponse.getPrimaryTerm(),
                            indexResponse.getVersion(),
                            sourceAndContent.v2(),
                            sourceAndContent.v1(),
                            indexSourceAsBytes
                        )
                    );
                }
            } else if (translatedResult == DocWriteResponse.Result.DELETED) {
                final DeleteResponse deleteResponse = operationResponse.getResponse();
                updateResponse = new UpdateResponse(
                    deleteResponse.getShardInfo(),
                    deleteResponse.getShardId(),
                    deleteResponse.getId(),
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    deleteResponse.getResult()
                );

                final GetResult getResult = UpdateHelper.extractGetResult(
                    updateRequest,
                    concreteIndex,
                    deleteResponse.getSeqNo(),
                    deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(),
                    translate.updateSourceContentType(),
                    null
                );

                updateResponse.setGetResult(getResult);
            } else {
                throw new IllegalArgumentException("unknown operation type: " + translatedResult);
            }
            response = BulkItemResponse.success(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
    }

    @Override
    protected void dispatchedShardOperationOnReplica(BulkShardRequest request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            final long startBulkTime = System.nanoTime();
            final Translog.Location location = performOnReplica(request, replica);
            replica.getBulkOperationListener().afterBulk(request.totalSizeInBytes(), System.nanoTime() - startBulkTime);
            return new WriteReplicaResult<>(request, location, null, replica, logger, postWriteAction);
        });
    }

    @Override
    protected long replicaOperationSize(BulkShardRequest request) {
        return request.ramBytesUsed();
    }

    @Override
    protected int replicaOperationCount(BulkShardRequest request) {
        return request.items().length;
    }

    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            final BulkItemRequest item = request.items()[i];
            final BulkItemResponse response = item.getPrimaryResponse();
            final Engine.Result operationResult;
            if (item.getPrimaryResponse().isFailed()) {
                if (response.getFailure().getSeqNo() == SequenceNumbers.UNASSIGNED_SEQ_NO) {
                    continue; // ignore replication as we didn't generate a sequence number for this request.
                }

                final long primaryTerm;
                if (response.getFailure().getTerm() == SequenceNumbers.UNASSIGNED_PRIMARY_TERM) {
                    // primary is on older version, just take the current primary term
                    primaryTerm = replica.getOperationPrimaryTerm();
                } else {
                    primaryTerm = response.getFailure().getTerm();
                }
                operationResult = replica.markSeqNoAsNoop(
                    response.getFailure().getSeqNo(),
                    primaryTerm,
                    response.getFailure().getMessage()
                );
            } else {
                if (response.getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                    continue; // ignore replication as it's a noop
                }
                assert response.getResponse().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO;
                operationResult = performOpOnReplica(response.getResponse(), item.request(), replica);
            }
            assert operationResult != null : "operation result must never be null when primary response has no failure";
            location = syncOperationResultOrThrow(operationResult, location);
        }
        return location;
    }

    private static Engine.Result performOpOnReplica(
        DocWriteResponse primaryResponse,
        DocWriteRequest<?> docWriteRequest,
        IndexShard replica
    ) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE, INDEX -> {
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final SourceToParse sourceToParse = new SourceToParse(
                    indexRequest.id(),
                    indexRequest.source(),
                    indexRequest.getContentType(),
                    indexRequest.routing(),
                    Map.of()
                );
                result = replica.applyIndexOperationOnReplica(
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(),
                    sourceToParse
                );
            }
            case DELETE -> {
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(
                    primaryResponse.getSeqNo(),
                    primaryResponse.getPrimaryTerm(),
                    primaryResponse.getVersion(),
                    deleteRequest.id()
                );
            }
            default -> {
                assert false : "Unexpected request operation type on replica: " + docWriteRequest + ";primary result: " + primaryResponse;
                throw new IllegalStateException("Unexpected request operation type on replica: " + docWriteRequest.opType().getLowercase());
            }
        }
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // Even though the primary waits on all nodes to ack the mapping changes to the master
            // (see MappingUpdatedAction.updateMappingOnMaster) we still need to protect against missing mappings
            // and wait for them. The reason is concurrent requests. Request r1 which has new field f triggers a
            // mapping update. Assume that that update is first applied on the primary, and only later on the replica
            // (it’s happening concurrently). Request r2, which now arrives on the primary and which also has the new
            // field f might see the updated mapping (on the primary), and will therefore proceed to be replicated
            // to the replica. When it arrives on the replica, there’s no guarantee that the replica has already
            // applied the new mapping, so there is no other option than to wait.
            throw new TransportReplicationAction.RetryOnReplicaException(
                replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate()
            );
        }
        return result;
    }
}
