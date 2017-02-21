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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
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
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.seqno.SequenceNumbersService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.action.bulk.BulkShardRequest;

import java.io.IOException;
import java.util.Map;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final UpdateHelper updateHelper;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.BULK);
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
        final IndexMetaData metaData = primary.indexSettings().getIndexMetaData();

        long[] preVersions = new long[request.items().length];
        VersionType[] preVersionTypes = new VersionType[request.items().length];
        Translog.Location location = null;
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            location = executeBulkItemRequest(metaData, primary, request, preVersions, preVersionTypes, location, requestIndex);
        }

        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        BulkShardResponse response = new BulkShardResponse(request.shardId(), responses);
        return new WritePrimaryResult<>(request, response, location, null, primary, logger);
    }

    /**
     * A struct-like holder for a bulk items reponse, result, and the resulting
     * replica operation to be executed.
     */
    static class BulkItemResultHolder {
        public final @Nullable DocWriteResponse response;
        public final @Nullable Engine.Result operationResult;
        public final BulkItemRequest replicaRequest;

        BulkItemResultHolder(@Nullable DocWriteResponse response,
                             @Nullable Engine.Result operationResult,
                             BulkItemRequest replicaRequest) {
            this.response = response;
            this.operationResult = operationResult;
            this.replicaRequest = replicaRequest;
        }
    }

    private BulkItemResultHolder executeIndexRequest(final IndexRequest indexRequest,
                                                     final BulkItemRequest bulkItemRequest,
                                                     final IndexShard primary) throws Exception {
        Engine.IndexResult indexResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
        if (indexResult.hasFailure()) {
            return new BulkItemResultHolder(null, indexResult, bulkItemRequest);
        } else {
            // update the version on request so it will happen on the replicas
            final long version = indexResult.getVersion();
            indexRequest.version(version);
            indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
            indexRequest.setSeqNo(indexResult.getSeqNo());
            assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());
            return new BulkItemResultHolder(new IndexResponse(primary.shardId(), indexRequest.type(), indexRequest.id(),
                                                              indexResult.getSeqNo(), indexResult.getVersion(),
                                                              indexResult.isCreated()),
                    indexResult, bulkItemRequest);
        }
    }

    private BulkItemResultHolder executeDeleteRequest(final DeleteRequest deleteRequest,
                                                      final BulkItemRequest bulkItemRequest,
                                                      final IndexShard primary) throws IOException {
        Engine.DeleteResult deleteResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
        if (deleteResult.hasFailure()) {
            return new BulkItemResultHolder(null, deleteResult, bulkItemRequest);
        } else {
            // update the request with the version so it will go to the replicas
            deleteRequest.versionType(deleteRequest.versionType().versionTypeForReplicationAndRecovery());
            deleteRequest.version(deleteResult.getVersion());
            deleteRequest.setSeqNo(deleteResult.getSeqNo());
            assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());
            return new BulkItemResultHolder(new DeleteResponse(primary.shardId(), deleteRequest.type(), deleteRequest.id(),
                                                               deleteResult.getSeqNo(), deleteResult.getVersion(),
                                                               deleteResult.isFound()),
                    deleteResult, bulkItemRequest);
        }
    }

    private Translog.Location updateReplicaRequest(BulkItemResultHolder bulkItemResult,
                                                   DocWriteRequest.OpType opType,
                                                   Translog.Location location,
                                                   BulkShardRequest request) {
        final Engine.Result operationResult = bulkItemResult.operationResult;
        final DocWriteResponse response = bulkItemResult.response;
        final BulkItemRequest replicaRequest = bulkItemResult.replicaRequest;

        if (operationResult == null) { // in case of noop update operation
            assert response.getResult() == DocWriteResponse.Result.NOOP : "only noop updates can have a null operation";
            replicaRequest.setIgnoreOnReplica();
            replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), opType, response));

        } else if (operationResult.hasFailure() == false) {
            // The operation was successful, advance the translog
            location = locationToSync(location, operationResult.getTranslogLocation());
            BulkItemResponse primaryResponse = new BulkItemResponse(replicaRequest.id(), opType, response);
            replicaRequest.setPrimaryResponse(primaryResponse);
            // set the ShardInfo to 0 so we can safely send it to the replicas. We won't use it in the real response though.
            primaryResponse.getResponse().setShardInfo(new ShardInfo());

        } else {
            DocWriteRequest docWriteRequest = replicaRequest.request();
            Exception failure = operationResult.getFailure();
            logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                            request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);

            // if it's a conflict failure, and we already executed the request on a primary (and we execute it
            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
            // then just use the response we got from the successful execution
            if (replicaRequest.getPrimaryResponse() == null || isConflictException(failure) == false) {
                replicaRequest.setIgnoreOnReplica();
                replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), docWriteRequest.opType(),
                                new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), failure)));
            }
        }
        return location;
    }

    /** Executes bulk item requests and handles request execution exceptions */
    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard primary,
                                                     BulkShardRequest request,
                                                     long[] preVersions, VersionType[] preVersionTypes,
                                                     Translog.Location location, int requestIndex) throws Exception {
        final DocWriteRequest itemRequest = request.items()[requestIndex].request();
        preVersions[requestIndex] = itemRequest.version();
        preVersionTypes[requestIndex] = itemRequest.versionType();
        final DocWriteRequest.OpType opType = itemRequest.opType();
        try {
            // execute item request
            final BulkItemResultHolder responseHolder;
            switch (opType) {
                case CREATE:
                case INDEX:
                    responseHolder = executeIndexRequest((IndexRequest) itemRequest, request.items()[requestIndex], primary);
                    break;
                case UPDATE:
                    responseHolder = executeUpdateRequest((UpdateRequest) itemRequest, primary, metaData, request, requestIndex);
                    break;
                case DELETE:
                    responseHolder = executeDeleteRequest((DeleteRequest) itemRequest, request.items()[requestIndex], primary);
                    break;
                default:
                    throw new IllegalStateException("unexpected opType [" + opType + "] found");
            }

            final BulkItemRequest replicaRequest = responseHolder.replicaRequest;

            // Update the bulk item request because update request execution can mutate the bulk item request
            request.items()[requestIndex] = replicaRequest;

            // Modify the replica request, if needed, and return a new translog location
            location = updateReplicaRequest(responseHolder, opType, location, request);

            assert replicaRequest.getPrimaryResponse() != null : "replica request must have a primary response";
            assert preVersionTypes[requestIndex] != null;

        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < requestIndex; j++) {
                    DocWriteRequest docWriteRequest = request.items()[j].request();
                    docWriteRequest.version(preVersions[j]);
                    docWriteRequest.versionType(preVersionTypes[j]);
                }
            }
            throw e;
        }
        return location;
    }

    private static boolean isConflictException(final Exception e) {
        return ExceptionsHelper.unwrapCause(e) instanceof VersionConflictEngineException;
    }

    /**
     * Executes update request, delegating to a index or delete operation after translation,
     * handles retries on version conflict and constructs update response
     * NOTE: reassigns bulk item request at <code>requestIndex</code> for replicas to
     * execute translated update request (NOOP update is an exception). NOOP updates are
     * indicated by returning a <code>null</code> operation in {@link BulkItemResultHolder}
     * */
    private BulkItemResultHolder executeUpdateRequest(UpdateRequest updateRequest, IndexShard primary,
                                                      IndexMetaData metaData, BulkShardRequest request,
                                                      int requestIndex) throws Exception {
        Engine.Result updateOperationResult = null;
        UpdateResponse updateResponse = null;
        BulkItemRequest replicaRequest = request.items()[requestIndex];
        int maxAttempts = updateRequest.retryOnConflict();
        for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
            final UpdateHelper.Result translate;
            // translate update request
            try {
                translate = updateHelper.prepare(updateRequest, primary, threadPool::estimatedTimeInMillis);
            } catch (Exception failure) {
                // we may fail translating a update to index or delete operation
                // we use index result to communicate failure while translating update request
                updateOperationResult = new Engine.IndexResult(failure, updateRequest.version(), SequenceNumbersService.UNASSIGNED_SEQ_NO);
                break; // out of retry loop
            }
            // execute translated update request
            switch (translate.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = translate.action();
                    MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                    indexRequest.process(mappingMd, request.index());
                    updateOperationResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    if (updateOperationResult.hasFailure() == false) {
                        // update the version on request so it will happen on the replicas
                        final long version = updateOperationResult.getVersion();
                        indexRequest.version(version);
                        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
                        indexRequest.setSeqNo(updateOperationResult.getSeqNo());
                        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());
                    }
                    break;
                case DELETED:
                    DeleteRequest deleteRequest = translate.action();
                    updateOperationResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
                    if (updateOperationResult.hasFailure() == false) {
                        // update the request with the version so it will go to the replicas
                        deleteRequest.versionType(deleteRequest.versionType().versionTypeForReplicationAndRecovery());
                        deleteRequest.version(updateOperationResult.getVersion());
                        deleteRequest.setSeqNo(updateOperationResult.getSeqNo());
                        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());
                    }
                    break;
                case NOOP:
                    primary.noopUpdate(updateRequest.type());
                    break;
                default: throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
            }
            if (updateOperationResult == null) {
                // this is a noop operation
                updateResponse = translate.action();
                break; // out of retry loop
            } else if (updateOperationResult.hasFailure() == false) {
                // enrich update response and
                // set translated update (index/delete) request for replica execution in bulk items
                switch (updateOperationResult.getOperationType()) {
                    case INDEX:
                        IndexRequest updateIndexRequest = translate.action();
                        final IndexResponse indexResponse = new IndexResponse(primary.shardId(),
                            updateIndexRequest.type(), updateIndexRequest.id(), updateOperationResult.getSeqNo(),
                            updateOperationResult.getVersion(), ((Engine.IndexResult) updateOperationResult).isCreated());
                        BytesReference indexSourceAsBytes = updateIndexRequest.source();
                        updateResponse = new UpdateResponse(indexResponse.getShardInfo(),
                            indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(), indexResponse.getSeqNo(),
                            indexResponse.getVersion(), indexResponse.getResult());
                        if ((updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) ||
                            (updateRequest.fields() != null && updateRequest.fields().length > 0)) {
                            Tuple<XContentType, Map<String, Object>> sourceAndContent =
                                XContentHelper.convertToMap(indexSourceAsBytes, true, updateIndexRequest.getContentType());
                            updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(),
                                indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                        }
                        // set translated request as replica request
                        replicaRequest = new BulkItemRequest(request.items()[requestIndex].id(), updateIndexRequest);
                        break;
                    case DELETE:
                        DeleteRequest updateDeleteRequest = translate.action();
                        DeleteResponse deleteResponse = new DeleteResponse(primary.shardId(),
                            updateDeleteRequest.type(), updateDeleteRequest.id(), updateOperationResult.getSeqNo(),
                            updateOperationResult.getVersion(), ((Engine.DeleteResult) updateOperationResult).isFound());
                        updateResponse = new UpdateResponse(deleteResponse.getShardInfo(),
                            deleteResponse.getShardId(), deleteResponse.getType(), deleteResponse.getId(), deleteResponse.getSeqNo(),
                            deleteResponse.getVersion(), deleteResponse.getResult());
                        updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest,
                            request.index(), deleteResponse.getVersion(), translate.updatedSourceAsMap(),
                            translate.updateSourceContentType(), null));
                        // set translated request as replica request
                        replicaRequest = new BulkItemRequest(request.items()[requestIndex].id(), updateDeleteRequest);
                        break;
                }
                assert (replicaRequest.request() instanceof IndexRequest
                    && ((IndexRequest) replicaRequest.request()).getSeqNo() != SequenceNumbersService.UNASSIGNED_SEQ_NO) ||
                    (replicaRequest.request() instanceof DeleteRequest
                        && ((DeleteRequest) replicaRequest.request()).getSeqNo() != SequenceNumbersService.UNASSIGNED_SEQ_NO);
                // successful operation
                break; // out of retry loop
            } else if (updateOperationResult.getFailure() instanceof VersionConflictEngineException == false) {
                // not a version conflict exception
                break; // out of retry loop
            }
        }
        return new BulkItemResultHolder(updateResponse, updateOperationResult, replicaRequest);
    }

    @Override
    public WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.isIgnoreOnReplica() == false) {
                DocWriteRequest docWriteRequest = item.request();
                // ensure request version is updated for replica operation during request execution in the primary
                assert docWriteRequest.versionType() == docWriteRequest.versionType().versionTypeForReplicationAndRecovery()
                        : "unexpected version in replica " + docWriteRequest.version();
                final Engine.Result operationResult;
                try {
                    switch (docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            operationResult = executeIndexRequestOnReplica((IndexRequest) docWriteRequest, replica);
                            break;
                        case DELETE:
                            operationResult = executeDeleteRequestOnReplica((DeleteRequest) docWriteRequest, replica);
                            break;
                        default:
                            throw new IllegalStateException("Unexpected request operation type on replica: "
                                + docWriteRequest.opType().getLowercase());
                    }
                    if (operationResult.hasFailure()) {
                        // check if any transient write operation failures should be bubbled up
                        Exception failure = operationResult.getFailure();
                        assert failure instanceof VersionConflictEngineException
                            || failure instanceof MapperParsingException
                            : "expected any one of [version conflict, mapper parsing, engine closed, index shard closed]" +
                            " failures. got " + failure;
                        if (!TransportActions.isShardNotAvailableException(failure)) {
                            throw failure;
                        }
                    } else {
                        location = locationToSync(location, operationResult.getTranslogLocation());
                    }
                } catch (Exception e) {
                    // if its not an ignore replica failure, we need to make sure to bubble up the failure
                    // so we will fail the shard
                    if (!TransportActions.isShardNotAvailableException(e)) {
                        throw e;
                    }
                }
            }
        }
        return new WriteReplicaResult<>(request, location, null, replica, logger);
    }

    private Translog.Location locationToSync(Translog.Location current, Translog.Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood
         * this might cross translog files which is ok since from the user perspective
         * the translog is like a tape where only the highest location needs to be fsynced
         * in order to sync all previous locations even though they are not in the same file.
         * When the translog rolls over files the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 : "translog locations are not increasing";
        return next;
    }

    /**
     * Execute the given {@link IndexRequest} on a replica shard, throwing a
     * {@link RetryOnReplicaException} if the operation needs to be re-tried.
     */
    public static Engine.IndexResult executeIndexRequestOnReplica(IndexRequest request, IndexShard replica) throws IOException {
        final ShardId shardId = replica.shardId();
        SourceToParse sourceToParse =
            SourceToParse.source(SourceToParse.Origin.REPLICA, shardId.getIndexName(), request.type(), request.id(), request.source(),
                request.getContentType()).routing(request.routing()).parent(request.parent());

        final Engine.Index operation;
        try {
            operation = replica.prepareIndexOnReplica(sourceToParse, request.getSeqNo(), request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
        } catch (MapperParsingException e) {
            return new Engine.IndexResult(e, request.version(), request.getSeqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
        }
        return replica.index(operation);
    }

    /** Utility method to prepare an index operation on primary shards */
    static Engine.Index prepareIndexOperationOnPrimary(IndexRequest request, IndexShard primary) {
        SourceToParse sourceToParse =
            SourceToParse.source(SourceToParse.Origin.PRIMARY, request.index(), request.type(), request.id(), request.source(),
                request.getContentType()).routing(request.routing()).parent(request.parent());
        return primary.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
    }

    /** Executes index operation on primary shard after updates mapping if dynamic mappings are found */
    public static Engine.IndexResult executeIndexRequestOnPrimary(IndexRequest request, IndexShard primary,
                                                                  MappingUpdatedAction mappingUpdatedAction) throws Exception {
        Engine.Index operation;
        try {
            operation = prepareIndexOperationOnPrimary(request, primary);
        } catch (MapperParsingException | IllegalArgumentException e) {
            return new Engine.IndexResult(e, request.version(), request.getSeqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = primary.shardId();
        if (update != null) {
            // can throw timeout exception when updating mappings or ISE for attempting to update default mappings
            // which are bubbled up
            try {
                mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), request.type(), update);
            } catch (IllegalArgumentException e) {
                // throws IAE on conflicts merging dynamic mappings
                return new Engine.IndexResult(e, request.version(), request.getSeqNo());
            }
            try {
                operation = prepareIndexOperationOnPrimary(request, primary);
            } catch (MapperParsingException | IllegalArgumentException e) {
                return new Engine.IndexResult(e, request.version(), request.getSeqNo());
            }
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new ReplicationOperation.RetryOnPrimaryException(shardId,
                        "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        return primary.index(operation);
    }

    public static Engine.DeleteResult executeDeleteRequestOnPrimary(DeleteRequest request, IndexShard primary) throws IOException {
        final Engine.Delete delete = primary.prepareDeleteOnPrimary(request.type(), request.id(), request.version(), request.versionType());
        return primary.delete(delete);
    }

    public static Engine.DeleteResult executeDeleteRequestOnReplica(DeleteRequest request, IndexShard replica) throws IOException {
        final Engine.Delete delete = replica.prepareDeleteOnReplica(request.type(), request.id(),
                request.getSeqNo(), request.primaryTerm(), request.version(), request.versionType());
        return replica.delete(delete);
    }
}
