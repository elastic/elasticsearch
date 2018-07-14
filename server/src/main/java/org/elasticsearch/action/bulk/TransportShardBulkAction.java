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

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private static final Logger logger = ESLoggerFactory.getLogger(TransportShardBulkAction.class);

    private final ThreadPool threadPool;
    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.WRITE);
        this.threadPool = threadPool;
        this.updateHelper = updateHelper;
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardResponse newResponseInstance() {
        return new BulkShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return false;
    }

    @Override
    protected void asyncShardWriteOperationOnPrimary(BulkShardRequest request, IndexShard primary,
                                                     ActionListener<WritePrimaryResult<BulkShardRequest, BulkShardResponse>> callback) {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, logger, threadPool.getThreadContext());
        performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis,
            new ConcreteMappingUpdatePerformer(), observer, clusterService.localNode(),
            callback);
    }

    public static void performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater, ClusterStateObserver observer, DiscoveryNode localNode,
        ActionListener<WritePrimaryResult<BulkShardRequest, BulkShardResponse>> callback) {
        PrimaryExecutionContext context = new PrimaryExecutionContext(request, primary);
        context.advance();
        performOnPrimary(context, updateHelper, nowInMillisSupplier, mappingUpdater, observer, localNode,
            () -> callback.onResponse(
                new WritePrimaryResult<>(request, context.buildShardResponse(), context.getLocationToSync(), null, primary, logger)),
            callback::onFailure
        );
    }

    private static void performOnPrimary(PrimaryExecutionContext context, UpdateHelper updateHelper,
                                         LongSupplier nowInMillisSupplier,
                                         MappingUpdatePerformer mappingUpdater, ClusterStateObserver observer,
                                         DiscoveryNode localNode,
                                         Runnable onComplete, Consumer<Exception> onFailure) {

        try {
            while (context.isAllFinalized() == false) {
                executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater);
                if (context.isFinalized()) {
                    context.advance();
                } else if (context.requiresWaitingForMappingUpdate()) {
                    PlainActionFuture<Void> waitingFuture = new PlainActionFuture<>();
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            waitingFuture.onResponse(null);
                        }

                        @Override
                        public void onClusterServiceClose() {
                            waitingFuture.onFailure(new NodeClosedException(localNode));
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            waitingFuture.onFailure(
                                new MapperException("timed out while waiting for a dynamic mapping update"));
                        }
                    });
                    try {
                        waitingFuture.get();
                    } catch (Exception e) {
                        context.failOnMappingUpdate(e);
                    }
                } else {
                    assert context.requiresImmediateRetry();
                }
            }
            onComplete.run();
        } catch (Exception e) {
            onFailure.accept(e);
        }
    }

    // Visible for unit testing
    /**
     * Creates a BulkItemResponse for the primary operation and returns it. If no bulk response is
     * needed (because one already exists and the operation failed), then return null.
     */
    static BulkItemResponse createPrimaryResponse(BulkItemResultHolder bulkItemResult,
                                                  final DocWriteRequest.OpType opType,
                                                  BulkShardRequest request) {
        final Engine.Result operationResult = bulkItemResult.operationResult;
        final DocWriteResponse response = bulkItemResult.response;
        final BulkItemRequest replicaRequest = bulkItemResult.replicaRequest;

        if (operationResult == null) { // in case of noop update operation
            assert response.getResult() == DocWriteResponse.Result.NOOP : "only noop updates can have a null operation";
            return new BulkItemResponse(replicaRequest.id(), opType, response);

        } else if (operationResult.getResultType() == Engine.Result.Type.SUCCESS) {
            BulkItemResponse primaryResponse = new BulkItemResponse(replicaRequest.id(), opType, response);
            // set a blank ShardInfo so we can safely send it to the replicas. We won't use it in the real response though.
            primaryResponse.getResponse().setShardInfo(new ShardInfo());
            return primaryResponse;

        } else if (operationResult.getResultType() == Engine.Result.Type.FAILURE) {
            DocWriteRequest<?> docWriteRequest = replicaRequest.request();
            Exception failure = operationResult.getFailure();
            if (isConflictException(failure)) {
                logger.trace(() -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
            } else {
                logger.debug(() -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
            }

            // if it's a conflict failure, and we already executed the request on a primary (and we execute it
            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
            // then just use the response we got from the failed execution
            if (replicaRequest.getPrimaryResponse() == null || isConflictException(failure) == false) {
                return new BulkItemResponse(replicaRequest.id(), docWriteRequest.opType(),
                        // Make sure to use request.index() here, if you
                        // use docWriteRequest.index() it will use the
                        // concrete index instead of an alias if used!
                        new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(),
                                failure, operationResult.getSeqNo()));
            } else {
                assert replicaRequest.getPrimaryResponse() != null : "replica request must have a primary response";
                return null;
            }
        } else {
            throw new AssertionError("unknown result type for " + request + ": " + operationResult.getResultType());
        }
    }

    /** Executes bulk item requests and handles request execution exceptions */
    static void executeBulkItemRequest(PrimaryExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
                                                    final MappingUpdatePerformer mappingUpdater) throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();
        final UpdateHelper.Result updateResult = translateRequestIfUpdate(context, updateHelper, nowInMillisSupplier);

        if (context.isFinalized()) {
            // translation shortcut the operation
            return;
        }

        if (context.getRequestToExecute() == null) {
            assert updateResult != null && updateResult.getResponseResult() == DocWriteResponse.Result.NOOP;
        } else if (context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE) {
            executeDeleteRequestOnPrimary(context, mappingUpdater);
        } else {
            executeIndexRequestOnPrimary(context, mappingUpdater);
        }

        if (opType == DocWriteRequest.OpType.UPDATE &&
            context.getExecutionResult().isFailed() &&
            isConflictException(context.getExecutionResult().getFailure().getCause())) {
            final UpdateRequest updateRequest = (UpdateRequest)context.getCurrent();
            if (context.getRetryCounter() < updateRequest.retryOnConflict()) {
                context.markForImmediateRetry();
            }
        }

        if (context.isOperationCompleted()) {
            finalizePrimaryOperationOnCompletion(context, opType, updateResult);
        }
    }

    private static void finalizePrimaryOperationOnCompletion(PrimaryExecutionContext context, DocWriteRequest.OpType opType,
                                                             UpdateHelper.Result updateResult) {
        final BulkItemResponse executionResult = context.getExecutionResult();
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest)context.getCurrent();
            context.finalize(
                processUpdateResponse(updateRequest, context.getConcreteIndex(), executionResult, updateResult));
        } else if (executionResult.isFailed()){
            final Exception failure = executionResult.getFailure().getCause();
            final DocWriteRequest docWriteRequest = context.getCurrent();
            if (TransportShardBulkAction.isConflictException(failure)) {
                logger.trace(() -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(), docWriteRequest.opType().getLowercase(), docWriteRequest), failure);
            } else {
                logger.debug(() -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                    context.getPrimary().shardId(), docWriteRequest.opType().getLowercase(), docWriteRequest), failure);
            }

            final BulkItemResponse primaryResponse;
            // if it's a conflict failure, and we already executed the request on a primary (and we execute it
            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
            // then just use the response we got from the failed execution
            if (TransportShardBulkAction.isConflictException(failure) && context.getPreviousPrimaryResponse() != null) {
                primaryResponse = context.getPreviousPrimaryResponse();
            } else {
                primaryResponse = executionResult;
            }
            context.finalize(primaryResponse);
        } else {
            context.finalize(executionResult);
        }
        assert context.isFinalized();
    }

    private static UpdateHelper.Result translateRequestIfUpdate(PrimaryExecutionContext context, UpdateHelper updateHelper,
                                                                LongSupplier nowInMillisSupplier) {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateHelper.Result updateResult;
            final UpdateRequest updateRequest = (UpdateRequest)context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result = new Engine.IndexResult(failure, updateRequest.version(), SequenceNumbers.UNASSIGNED_SEQ_NO);
                context.setRequestToExecute(updateRequest);
                context.markOperationAsCompleted(result);
                context.finalize(context.getExecutionResult());
                return null;
            }
            // execute translated update request
            switch (updateResult.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = updateResult.action();
                    IndexMetaData metaData = context.getPrimary().indexSettings().getIndexMetaData();
                    MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                    indexRequest.process(metaData.getCreationVersion(), mappingMd, updateRequest.concreteIndex());
                    context.setRequestToExecute(indexRequest);
                    break;
                case DELETED:
                    context.setRequestToExecute(updateResult.action());
                    break;
                case NOOP:
                    context.markOperationAsNoOp();
                    break;
                default: throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
            return updateResult;
        } else {
            context.setRequestToExecute(context.getCurrent());
            return null;
        }
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    static BulkItemResponse processUpdateResponse(final UpdateRequest updateRequest, final String concreteIndex,
                                                  BulkItemResponse operationResponse,
                                                  final UpdateHelper.Result translate) {

        DocWriteResponse.Result translatedResult = translate.getResponseResult();
        if (operationResponse.isFailed()) {
            return operationResponse;
        }

        final UpdateResponse updateResponse;
        if (translatedResult == DocWriteResponse.Result.NOOP) {
            updateResponse = translate.action();
        } else if (translatedResult == DocWriteResponse.Result.UPDATED || translatedResult == DocWriteResponse.Result.UPDATED) {
            final IndexRequest updateIndexRequest = translate.action();
            final IndexResponse indexResponse = operationResponse.getResponse();
            updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(),
                indexResponse.getType(), indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(),
                indexResponse.getVersion(), indexResponse.getResult());

            if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                final Tuple<XContentType, Map<String, Object>> sourceAndContent =
                    XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());
                updateResponse.setGetResult(UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                    indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
            }
        } else if (translatedResult == DocWriteResponse.Result.DELETED) {
            final DeleteResponse deleteResponse = operationResponse.getResponse();
            updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(),
                deleteResponse.getType(), deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(),
                deleteResponse.getVersion(), deleteResponse.getResult());

            final GetResult getResult = UpdateHelper.extractGetResult(updateRequest, concreteIndex, deleteResponse.getVersion(),
                translate.updatedSourceAsMap(), translate.updateSourceContentType(), null);

            updateResponse.setGetResult(getResult);
        } else {
            throw new IllegalArgumentException("unknown operation type: " + translatedResult);
        }
        return new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
    }


    /** Modes for executing item request on replica depending on corresponding primary execution result */
    public enum ReplicaItemExecutionMode {

        /**
         * When primary execution succeeded
         */
        NORMAL,

        /**
         * When primary execution failed before sequence no was generated
         * or primary execution was a noop (only possible when request is originating from pre-6.0 nodes)
         */
        NOOP,

        /**
         * When primary execution failed after sequence no was generated
         */
        FAILURE
    }

    /**
     * Determines whether a bulk item request should be executed on the replica.
     * @return {@link ReplicaItemExecutionMode#NORMAL} upon normal primary execution with no failures
     * {@link ReplicaItemExecutionMode#FAILURE} upon primary execution failure after sequence no generation
     * {@link ReplicaItemExecutionMode#NOOP} upon primary execution failure before sequence no generation or
     * when primary execution resulted in noop (only possible for write requests from pre-6.0 nodes)
     */
    static ReplicaItemExecutionMode replicaItemExecutionMode(final BulkItemRequest request, final int index) {
        final BulkItemResponse primaryResponse = request.getPrimaryResponse();
        assert primaryResponse != null : "expected primary response to be set for item [" + index + "] request [" + request.request() + "]";
        if (primaryResponse.isFailed()) {
            return primaryResponse.getFailure().getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO
                    ? ReplicaItemExecutionMode.FAILURE // we have a seq no generated with the failure, replicate as no-op
                    : ReplicaItemExecutionMode.NOOP; // no seq no generated, ignore replication
        } else {
            // TODO: once we know for sure that every operation that has been processed on the primary is assigned a seq#
            // (i.e., all nodes on the cluster are on v6.0.0 or higher) we can use the existence of a seq# to indicate whether
            // an operation should be processed or be treated as a noop. This means we could remove this method and the
            // ReplicaItemExecutionMode enum and have a simple boolean check for seq != UNASSIGNED_SEQ_NO which will work for
            // both failures and indexing operations.
            return primaryResponse.getResponse().getResult() != DocWriteResponse.Result.NOOP
                    ? ReplicaItemExecutionMode.NORMAL // execution successful on primary
                    : ReplicaItemExecutionMode.NOOP; // ignore replication
        }
    }

    @Override
    public WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        final Translog.Location location = performOnReplica(request, replica);
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    public static Translog.Location performOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            final Engine.Result operationResult;
            DocWriteRequest<?> docWriteRequest = item.request();
            switch (replicaItemExecutionMode(item, i)) {
                case NORMAL:
                    final DocWriteResponse primaryResponse = item.getPrimaryResponse().getResponse();
                    operationResult = performOpOnReplica(primaryResponse, docWriteRequest, replica);
                    assert operationResult != null : "operation result must never be null when primary response has no failure";
                    location = syncOperationResultOrThrow(operationResult, location);
                    break;
                case NOOP:
                    break;
                case FAILURE:
                    final BulkItemResponse.Failure failure = item.getPrimaryResponse().getFailure();
                    assert failure.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "seq no must be assigned";
                    operationResult = replica.markSeqNoAsNoop(failure.getSeqNo(), failure.getMessage());
                    assert operationResult != null : "operation result must never be null when primary response has no failure";
                    location = syncOperationResultOrThrow(operationResult, location);
                    break;
                default:
                    throw new IllegalStateException("illegal replica item execution mode for: " + docWriteRequest);
            }
        }
        return location;
    }

    private static Engine.Result performOpOnReplica(DocWriteResponse primaryResponse, DocWriteRequest<?> docWriteRequest,
                                                    IndexShard replica) throws Exception {
        final Engine.Result result;
        switch (docWriteRequest.opType()) {
            case CREATE:
            case INDEX:
                final IndexRequest indexRequest = (IndexRequest) docWriteRequest;
                final ShardId shardId = replica.shardId();
                final SourceToParse sourceToParse =
                    SourceToParse.source(shardId.getIndexName(),
                        indexRequest.type(), indexRequest.id(), indexRequest.source(), indexRequest.getContentType())
                        .routing(indexRequest.routing());
                result = replica.applyIndexOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getVersion(),
                    indexRequest.versionType().versionTypeForReplicationAndRecovery(), indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(), sourceToParse);
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result =  replica.applyDeleteOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getVersion(),
                    deleteRequest.type(), deleteRequest.id(), deleteRequest.versionType().versionTypeForReplicationAndRecovery());
                break;
            default:
                throw new IllegalStateException("Unexpected request operation type on replica: "
                    + docWriteRequest.opType().getLowercase());
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
            throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
                "Mappings are not available on the replica yet, triggered update: " + result.getRequiredMappingUpdate());
        }
        return result;
    }

    /** Executes index operation on primary shard after updates mapping if dynamic mappings are found */
    static void executeIndexRequestOnPrimary(PrimaryExecutionContext context,
                                             MappingUpdatePerformer mappingUpdater) throws Exception {
        final IndexRequest request = context.getRequestToExecute();
        final IndexShard primary = context.getPrimary();
        final SourceToParse sourceToParse =
            SourceToParse.source(request.index(), request.type(), request.id(), request.source(), request.getContentType())
                .routing(request.routing());
        executeOnPrimaryWhileHandlingMappingUpdates(context,
            () ->
                primary.applyIndexOperationOnPrimary(request.version(), request.versionType(), sourceToParse,
                    request.getAutoGeneratedTimestamp(), request.isRetry()),
            e -> new Engine.IndexResult(e, request.version()),
            context::markOperationAsCompleted,
            mapping -> mappingUpdater.updateMappings(mapping, primary.shardId(), request.type()));
    }

    private static void executeDeleteRequestOnPrimary(PrimaryExecutionContext context,
                                                                     MappingUpdatePerformer mappingUpdater) throws Exception {
        final DeleteRequest request = (DeleteRequest) context.getCurrent();
        final IndexShard primary = context.getPrimary();
        executeOnPrimaryWhileHandlingMappingUpdates(context,
            () -> primary.applyDeleteOperationOnPrimary(request.version(), request.type(), request.id(), request.versionType()),
            e -> new Engine.DeleteResult(e, request.version()),
            context::markOperationAsCompleted,
            mapping -> mappingUpdater.updateMappings(mapping, primary.shardId(), request.type()));
    }

    private static <T extends Engine.Result> void executeOnPrimaryWhileHandlingMappingUpdates(
        PrimaryExecutionContext context, CheckedSupplier<T, IOException> toExecute,
        Function<Exception, T> exceptionToResult, Consumer<T> onComplete, Consumer<Mapping> mappingUpdater)
        throws IOException {
        T result = toExecute.get();
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // try to update the mappings and try again.
            try {
                mappingUpdater.accept(result.getRequiredMappingUpdate());
            } catch (Exception e) {
                // failure to update the mapping should translate to a failure of specific requests. Other requests
                // still need to be executed and replicated.
                onComplete.accept(exceptionToResult.apply(e));
                return;
            }

            // TODO - we can fall back to a wait for cluster state update but I'm keeping the logic the same for now
            result = toExecute.get();

            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                // double mapping update. We assume that the successful mapping update wasn't yet processed on the node
                // and retry the entire request again.
                context.markAsRequiringMappingUpdate();
            } else {
                onComplete.accept(result);
            }
        } else {
            onComplete.accept(result);
        }
    }

    class ConcreteMappingUpdatePerformer implements MappingUpdatePerformer {

        @Override
        public void updateMappings(final Mapping update, final ShardId shardId, final String type) {
            assert update != null;
            assert shardId != null;
            // can throw timeout exception when updating mappings or ISE for attempting to
            // update default mappings which are bubbled up
            mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), type, update);
        }
    }
}
