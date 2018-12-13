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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
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

    private static final Logger logger = LogManager.getLogger(TransportShardBulkAction.class);

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
    protected TransportRequestOptions transportOptions(Settings settings) {
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
    protected WritePrimaryResult<BulkShardRequest, BulkShardResponse> shardOperationOnPrimary(BulkShardRequest request, IndexShard primary)
        throws Exception {
        ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout(), logger, threadPool.getThreadContext());
        CheckedRunnable<Exception> waitForMappingUpdate = () -> {
            PlainActionFuture<Void> waitingFuture = new PlainActionFuture<>();
            observer.waitForNextChange(new ClusterStateObserver.Listener() {
                @Override
                public void onNewClusterState(ClusterState state) {
                    waitingFuture.onResponse(null);
                }

                @Override
                public void onClusterServiceClose() {
                    waitingFuture.onFailure(new NodeClosedException(clusterService.localNode()));
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    waitingFuture.onFailure(
                        new MapperException("timed out while waiting for a dynamic mapping update"));
                }
            });
            waitingFuture.get();
        };
        return performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis,
            new ConcreteMappingUpdatePerformer(), waitForMappingUpdate);
    }

    public static WritePrimaryResult<BulkShardRequest, BulkShardResponse> performOnPrimary(
        BulkShardRequest request,
        IndexShard primary,
        UpdateHelper updateHelper,
        LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater,
        CheckedRunnable<Exception> waitForMappingUpdate) throws Exception {
        BulkPrimaryExecutionContext context = new BulkPrimaryExecutionContext(request, primary);
        return performOnPrimary(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate);
    }

    private static WritePrimaryResult<BulkShardRequest, BulkShardResponse> performOnPrimary(
        BulkPrimaryExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
        MappingUpdatePerformer mappingUpdater, CheckedRunnable<Exception> waitForMappingUpdate) throws Exception {

        while (context.hasMoreOperationsToExecute()) {
            executeBulkItemRequest(context, updateHelper, nowInMillisSupplier, mappingUpdater, waitForMappingUpdate);
            assert context.isInitial(); // either completed and moved to next or reset
        }
        return new WritePrimaryResult<>(context.getBulkShardRequest(), context.buildShardResponse(), context.getLocationToSync(),
            null, context.getPrimary(), logger);
    }

    /** Executes bulk item requests and handles request execution exceptions */
    static void executeBulkItemRequest(BulkPrimaryExecutionContext context, UpdateHelper updateHelper, LongSupplier nowInMillisSupplier,
                                       MappingUpdatePerformer mappingUpdater, CheckedRunnable<Exception> waitForMappingUpdate)
        throws Exception {
        final DocWriteRequest.OpType opType = context.getCurrent().opType();

        final UpdateHelper.Result updateResult;
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            try {
                updateResult = updateHelper.prepare(updateRequest, context.getPrimary(), nowInMillisSupplier);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                final Engine.Result result = new Engine.IndexResult(failure, updateRequest.version(), SequenceNumbers.UNASSIGNED_SEQ_NO);
                context.setRequestToExecute(updateRequest);
                context.markOperationAsExecuted(result);
                context.markAsCompleted(context.getExecutionResult());
                return;
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
                    context.markOperationAsNoOp(updateResult.action());
                    context.markAsCompleted(context.getExecutionResult());
                    return;
                default:
                    throw new IllegalStateException("Illegal update operation " + updateResult.getResponseResult());
            }
        } else {
            context.setRequestToExecute(context.getCurrent());
            updateResult = null;
        }

        assert context.getRequestToExecute() != null; // also checks that we're in TRANSLATED state

        if (context.getRequestToExecute().opType() == DocWriteRequest.OpType.DELETE) {
            executeDeleteRequestOnPrimary(context, mappingUpdater);
        } else {
            executeIndexRequestOnPrimary(context, mappingUpdater);
        }

        if (context.requiresWaitingForMappingUpdate()) {
            try {
                waitForMappingUpdate.run();
                context.resetForExecutionForRetry();
            } catch (Exception e) {
                context.failOnMappingUpdate(e);
            }
            return;
        }

        assert context.isOperationExecuted();

        if (opType == DocWriteRequest.OpType.UPDATE &&
            context.getExecutionResult().isFailed() &&
            isConflictException(context.getExecutionResult().getFailure().getCause())) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            if (context.getRetryCounter() < updateRequest.retryOnConflict()) {
                context.resetForExecutionForRetry();
                return;
            }
        }

        finalizePrimaryOperationOnCompletion(context, opType, updateResult);
    }

    private static void finalizePrimaryOperationOnCompletion(BulkPrimaryExecutionContext context, DocWriteRequest.OpType opType,
                                                             UpdateHelper.Result updateResult) {
        final BulkItemResponse executionResult = context.getExecutionResult();
        if (opType == DocWriteRequest.OpType.UPDATE) {
            final UpdateRequest updateRequest = (UpdateRequest) context.getCurrent();
            context.markAsCompleted(
                processUpdateResponse(updateRequest, context.getConcreteIndex(), executionResult, updateResult));
        } else if (executionResult.isFailed()) {
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
            context.markAsCompleted(primaryResponse);
        } else {
            context.markAsCompleted(executionResult);
        }
        assert context.isInitial();
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

        final BulkItemResponse response;
        DocWriteResponse.Result translatedResult = translate.getResponseResult();
        if (operationResponse.isFailed()) {
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, operationResponse.getFailure());
        } else {

            final UpdateResponse updateResponse;
            if (translatedResult == DocWriteResponse.Result.CREATED || translatedResult == DocWriteResponse.Result.UPDATED) {
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
            response = new BulkItemResponse(operationResponse.getItemId(), DocWriteRequest.OpType.UPDATE, updateResponse);
        }
        return response;
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
     *
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
                    indexRequest.getAutoGeneratedTimestamp(), indexRequest.isRetry(), sourceToParse);
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result = replica.applyDeleteOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getVersion(),
                    deleteRequest.type(), deleteRequest.id());
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
    private static void executeIndexRequestOnPrimary(BulkPrimaryExecutionContext context,
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
            e -> primary.getFailedIndexResult(e, request.version()),
            context::markOperationAsExecuted,
            mapping -> mappingUpdater.updateMappings(mapping, primary.shardId(), request.type()));
    }

    private static void executeDeleteRequestOnPrimary(BulkPrimaryExecutionContext context,
                                                      MappingUpdatePerformer mappingUpdater) throws Exception {
        final DeleteRequest request = context.getRequestToExecute();
        final IndexShard primary = context.getPrimary();
        executeOnPrimaryWhileHandlingMappingUpdates(context,
            () -> primary.applyDeleteOperationOnPrimary(request.version(), request.type(), request.id(), request.versionType()),
            e -> primary.getFailedDeleteResult(e, request.version()),
            context::markOperationAsExecuted,
            mapping -> mappingUpdater.updateMappings(mapping, primary.shardId(), request.type()));
    }

    private static <T extends Engine.Result> void executeOnPrimaryWhileHandlingMappingUpdates(
        BulkPrimaryExecutionContext context, CheckedSupplier<T, IOException> toExecute,
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
