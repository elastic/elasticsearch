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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
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
    public WritePrimaryResult<BulkShardRequest, BulkShardResponse> shardOperationOnPrimary(
            BulkShardRequest request, IndexShard primary) throws Exception {
        return performOnPrimary(request, primary, updateHelper, threadPool::absoluteTimeInMillis, new ConcreteMappingUpdatePerformer());
    }

    public static WritePrimaryResult<BulkShardRequest, BulkShardResponse> performOnPrimary(
            BulkShardRequest request,
            IndexShard primary,
            UpdateHelper updateHelper,
            LongSupplier nowInMillisSupplier,
            MappingUpdatePerformer mappingUpdater) throws Exception {
        final IndexMetaData metaData = primary.indexSettings().getIndexMetaData();
        Translog.Location location = null;
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            if (isAborted(request.items()[requestIndex].getPrimaryResponse()) == false) {
                location = executeBulkItemRequest(metaData, primary, request, location, requestIndex,
                    updateHelper, nowInMillisSupplier, mappingUpdater);
            }
        }
        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        BulkShardResponse response = new BulkShardResponse(request.shardId(), responses);
        return new WritePrimaryResult<>(request, response, location, null, primary, logger);
    }

    private static BulkItemResultHolder executeIndexRequest(final IndexRequest indexRequest,
                                                            final BulkItemRequest bulkItemRequest,
                                                            final IndexShard primary,
                                                            final MappingUpdatePerformer mappingUpdater) throws Exception {
        Engine.IndexResult indexResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdater);
        switch (indexResult.getResultType()) {
            case SUCCESS:
                IndexResponse response = new IndexResponse(primary.shardId(), indexRequest.type(), indexRequest.id(),
                    indexResult.getSeqNo(), indexResult.getTerm(), indexResult.getVersion(), indexResult.isCreated());
                return new BulkItemResultHolder(response, indexResult, bulkItemRequest);
            case FAILURE:
                return new BulkItemResultHolder(null, indexResult, bulkItemRequest);
            default:
                throw new AssertionError("unknown result type for " + indexRequest + ": " + indexResult.getResultType());
        }
    }

    private static BulkItemResultHolder executeDeleteRequest(final DeleteRequest deleteRequest,
                                                             final BulkItemRequest bulkItemRequest,
                                                             final IndexShard primary,
                                                             final MappingUpdatePerformer mappingUpdater) throws Exception {
        Engine.DeleteResult deleteResult = executeDeleteRequestOnPrimary(deleteRequest, primary, mappingUpdater);
        switch (deleteResult.getResultType()) {
            case SUCCESS:
                DeleteResponse response = new DeleteResponse(primary.shardId(), deleteRequest.type(), deleteRequest.id(),
                    deleteResult.getSeqNo(), deleteResult.getTerm(), deleteResult.getVersion(), deleteResult.isFound());
                return new BulkItemResultHolder(response, deleteResult, bulkItemRequest);
            case FAILURE:
                return new BulkItemResultHolder(null, deleteResult, bulkItemRequest);
            case MAPPING_UPDATE_REQUIRED:
                throw new AssertionError("delete operation leaked a mapping update " + deleteRequest);
            default:
                throw new AssertionError("unknown result type for " + deleteRequest + ": " + deleteResult.getResultType());
        }
    }

    static Translog.Location calculateTranslogLocation(final Translog.Location originalLocation,
                                                       final BulkItemResultHolder bulkItemResult) {
        final Engine.Result operationResult = bulkItemResult.operationResult;
        if (operationResult != null && operationResult.getResultType() == Engine.Result.Type.SUCCESS) {
            return locationToSync(originalLocation, operationResult.getTranslogLocation());
        } else {
            return originalLocation;
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
    static Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard primary,
                                                    BulkShardRequest request, Translog.Location location,
                                                    int requestIndex, UpdateHelper updateHelper,
                                                    LongSupplier nowInMillisSupplier,
                                                    final MappingUpdatePerformer mappingUpdater) throws Exception {
        final DocWriteRequest<?> itemRequest = request.items()[requestIndex].request();
        final DocWriteRequest.OpType opType = itemRequest.opType();
        final BulkItemResultHolder responseHolder;
        switch (itemRequest.opType()) {
            case CREATE:
            case INDEX:
                responseHolder = executeIndexRequest((IndexRequest) itemRequest,
                        request.items()[requestIndex], primary, mappingUpdater);
                break;
            case UPDATE:
                responseHolder = executeUpdateRequest((UpdateRequest) itemRequest, primary, metaData, request,
                        requestIndex, updateHelper, nowInMillisSupplier, mappingUpdater);
                break;
            case DELETE:
                responseHolder = executeDeleteRequest((DeleteRequest) itemRequest, request.items()[requestIndex], primary, mappingUpdater);
                break;
            default: throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
        }

        final BulkItemRequest replicaRequest = responseHolder.replicaRequest;

        // update the bulk item request because update request execution can mutate the bulk item request
        request.items()[requestIndex] = replicaRequest;

        // Retrieve the primary response, and update the replica request with the primary's response
        BulkItemResponse primaryResponse = createPrimaryResponse(responseHolder, opType, request);
        if (primaryResponse != null) {
            replicaRequest.setPrimaryResponse(primaryResponse);
        }

        // Update the translog with the new location, if needed
        return calculateTranslogLocation(location, responseHolder);
    }

    private static boolean isAborted(BulkItemResponse response) {
        return response != null && response.isFailed() && response.getFailure().isAborted();
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Creates a new bulk item result from the given requests and result of performing the update operation on the shard.
     */
    static BulkItemResultHolder processUpdateResponse(final UpdateRequest updateRequest, final String concreteIndex,
                                                      final Engine.Result result, final UpdateHelper.Result translate,
                                                      final IndexShard primary, final int bulkReqId) {
        assert result.getSeqNo() != SequenceNumbers.UNASSIGNED_SEQ_NO : "failed result should not have a sequence number";

        Engine.Operation.TYPE opType = result.getOperationType();

        final UpdateResponse updateResponse;
        final BulkItemRequest replicaRequest;

        // enrich update response and set translated update (index/delete) request for replica execution in bulk items
        if (opType == Engine.Operation.TYPE.INDEX) {
            assert result instanceof Engine.IndexResult : result.getClass();
            final IndexRequest updateIndexRequest = translate.action();
            final IndexResponse indexResponse = new IndexResponse(primary.shardId(), updateIndexRequest.type(), updateIndexRequest.id(),
                    result.getSeqNo(), result.getTerm(), result.getVersion(), ((Engine.IndexResult) result).isCreated());
            updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(), indexResponse.getType(),
                    indexResponse.getId(), indexResponse.getSeqNo(), indexResponse.getPrimaryTerm(), indexResponse.getVersion(),
                    indexResponse.getResult());

            if (updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) {
                final BytesReference indexSourceAsBytes = updateIndexRequest.source();
                final Tuple<XContentType, Map<String, Object>> sourceAndContent =
                        XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());
                updateResponse.setGetResult(UpdateHelper.extractGetResult(updateRequest, concreteIndex,
                                indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
            }
            // set translated request as replica request
            replicaRequest = new BulkItemRequest(bulkReqId, updateIndexRequest);

        } else if (opType == Engine.Operation.TYPE.DELETE) {
            assert result instanceof Engine.DeleteResult : result.getClass();
            final DeleteRequest updateDeleteRequest = translate.action();

            final DeleteResponse deleteResponse = new DeleteResponse(primary.shardId(), updateDeleteRequest.type(), updateDeleteRequest.id(),
                    result.getSeqNo(), result.getTerm(), result.getVersion(), ((Engine.DeleteResult) result).isFound());

            updateResponse = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(),
                    deleteResponse.getType(), deleteResponse.getId(), deleteResponse.getSeqNo(), deleteResponse.getPrimaryTerm(),
                    deleteResponse.getVersion(), deleteResponse.getResult());

            final GetResult getResult = UpdateHelper.extractGetResult(updateRequest, concreteIndex, deleteResponse.getVersion(),
                    translate.updatedSourceAsMap(), translate.updateSourceContentType(), null);

            updateResponse.setGetResult(getResult);
            // set translated request as replica request
            replicaRequest = new BulkItemRequest(bulkReqId, updateDeleteRequest);

        } else {
            throw new IllegalArgumentException("unknown operation type: " + opType);
        }

        return new BulkItemResultHolder(updateResponse, result, replicaRequest);
    }

    /**
     * Executes update request once, delegating to a index or delete operation after translation.
     * NOOP updates are indicated by returning a <code>null</code> operation in {@link BulkItemResultHolder}
     */
    static BulkItemResultHolder executeUpdateRequestOnce(UpdateRequest updateRequest, IndexShard primary,
                                                         IndexMetaData metaData, String concreteIndex,
                                                         UpdateHelper updateHelper, LongSupplier nowInMillis,
                                                         BulkItemRequest primaryItemRequest, int bulkReqId,
                                                         final MappingUpdatePerformer mappingUpdater) throws Exception {
        final UpdateHelper.Result translate;
        // translate update request
        try {
            translate = updateHelper.prepare(updateRequest, primary, nowInMillis);
        } catch (Exception failure) {
            // we may fail translating a update to index or delete operation
            // we use index result to communicate failure while translating update request
            final Engine.Result result = primary.getFailedIndexResult(failure, updateRequest.version());
            return new BulkItemResultHolder(null, result, primaryItemRequest);
        }

        final Engine.Result result;
        // execute translated update request
        switch (translate.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = translate.action();
                MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                indexRequest.process(metaData.getCreationVersion(), mappingMd, concreteIndex);
                result = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdater);
                break;
            case DELETED:
                DeleteRequest deleteRequest = translate.action();
                result = executeDeleteRequestOnPrimary(deleteRequest, primary, mappingUpdater);
                break;
            case NOOP:
                primary.noopUpdate(updateRequest.type());
                result = null;
                break;
            default: throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
        }

        if (result == null) {
            // this is a noop operation
            final UpdateResponse updateResponse = translate.action();
            return new BulkItemResultHolder(updateResponse, result, primaryItemRequest);
        } else if (result.getResultType() == Engine.Result.Type.FAILURE) {
            // There was a result, and the result was a failure
            return new BulkItemResultHolder(null, result, primaryItemRequest);
        } else if (result.getResultType() == Engine.Result.Type.SUCCESS) {
            // It was successful, we need to construct the response and return it
            return processUpdateResponse(updateRequest, concreteIndex, result, translate, primary, bulkReqId);
        } else {
            throw new AssertionError("unknown result type for " + updateRequest + ": " + result.getResultType());
        }
    }

    /**
     * Executes update request, delegating to a index or delete operation after translation,
     * handles retries on version conflict and constructs update response
     * NOOP updates are indicated by returning a <code>null</code> operation
     * in {@link BulkItemResultHolder}
     */
    private static BulkItemResultHolder executeUpdateRequest(UpdateRequest updateRequest, IndexShard primary,
                                                             IndexMetaData metaData, BulkShardRequest request,
                                                             int requestIndex, UpdateHelper updateHelper,
                                                             LongSupplier nowInMillis,
                                                             final MappingUpdatePerformer mappingUpdater) throws Exception {
        BulkItemRequest primaryItemRequest = request.items()[requestIndex];
        assert primaryItemRequest.request() == updateRequest
                : "expected bulk item request to contain the original update request, got: " +
                primaryItemRequest.request() + " and " + updateRequest;

        BulkItemResultHolder holder = null;
        // There must be at least one attempt
        int maxAttempts = Math.max(1, updateRequest.retryOnConflict());
        for (int attemptCount = 0; attemptCount < maxAttempts; attemptCount++) {

            holder = executeUpdateRequestOnce(updateRequest, primary, metaData, request.index(), updateHelper,
                    nowInMillis, primaryItemRequest, request.items()[requestIndex].id(), mappingUpdater);

            // It was either a successful request, or it was a non-conflict failure
            if (holder.isVersionConflict() == false) {
                return holder;
            }
        }
        // We ran out of tries and haven't returned a valid bulk item response, so return the last one generated
        return holder;
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
                    indexRequest.getAutoGeneratedTimestamp(), indexRequest.isRetry(), sourceToParse);
                break;
            case DELETE:
                DeleteRequest deleteRequest = (DeleteRequest) docWriteRequest;
                result =  replica.applyDeleteOperationOnReplica(primaryResponse.getSeqNo(), primaryResponse.getVersion(),
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
    static Engine.IndexResult executeIndexRequestOnPrimary(IndexRequest request, IndexShard primary,
                                                           MappingUpdatePerformer mappingUpdater) throws Exception {
        final SourceToParse sourceToParse =
            SourceToParse.source(request.index(), request.type(), request.id(), request.source(), request.getContentType())
                .routing(request.routing());
        return executeOnPrimaryWhileHandlingMappingUpdates(primary.shardId(), request.type(),
            () ->
                primary.applyIndexOperationOnPrimary(request.version(), request.versionType(), sourceToParse,
                    request.getAutoGeneratedTimestamp(), request.isRetry()),
            e -> primary.getFailedIndexResult(e, request.version()),
            mappingUpdater);
    }

    private static Engine.DeleteResult executeDeleteRequestOnPrimary(DeleteRequest request, IndexShard primary,
                                                                     MappingUpdatePerformer mappingUpdater) throws Exception {
        return executeOnPrimaryWhileHandlingMappingUpdates(primary.shardId(), request.type(),
            () -> primary.applyDeleteOperationOnPrimary(request.version(), request.type(), request.id(), request.versionType()),
            e -> primary.getFailedDeleteResult(e, request.version()),
            mappingUpdater);
    }

    private static <T extends Engine.Result> T executeOnPrimaryWhileHandlingMappingUpdates(ShardId shardId, String type,
                                                                                           CheckedSupplier<T, IOException> toExecute,
                                                                                           Function<Exception, T> onError,
                                                                                           MappingUpdatePerformer mappingUpdater)
        throws IOException {
        T result = toExecute.get();
        if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
            // try to update the mappings and try again.
            try {
                mappingUpdater.updateMappings(result.getRequiredMappingUpdate(), shardId, type);
            } catch (Exception e) {
                // failure to update the mapping should translate to a failure of specific requests. Other requests
                // still need to be executed and replicated.
                return onError.apply(e);
            }

            result = toExecute.get();

            if (result.getResultType() == Engine.Result.Type.MAPPING_UPDATE_REQUIRED) {
                // double mapping update. We assume that the successful mapping update wasn't yet processed on the node
                // and retry the entire request again.
                throw new ReplicationOperation.RetryOnPrimaryException(shardId,
                    "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        assert result.getFailure() instanceof ReplicationOperation.RetryOnPrimaryException == false :
            "IndexShard shouldn't use RetryOnPrimaryException. got " + result.getFailure();
        return result;

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
