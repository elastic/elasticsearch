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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final UpdateHelper updateHelper;
    private final boolean allowIdGeneration;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.BULK);
        this.updateHelper = updateHelper;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
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

    /** Executes bulk item requests and handles request execution exceptions */
    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard primary,
                                                     BulkShardRequest request,
                                                     long[] preVersions, VersionType[] preVersionTypes,
                                                     Translog.Location location, int requestIndex) throws Exception {
        final DocWriteRequest itemRequest = request.items()[requestIndex].request();
        preVersions[requestIndex] = itemRequest.version();
        preVersionTypes[requestIndex] = itemRequest.versionType();
        DocWriteRequest.OpType opType = itemRequest.opType();
        try {
            // execute item request
            final Engine.Result operationResult;
            final DocWriteResponse response;
            final BulkItemRequest replicaRequest;
            switch (itemRequest.opType()) {
                case CREATE:
                case INDEX:
                    final IndexRequest indexRequest = (IndexRequest) itemRequest;
                    Engine.IndexResult indexResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    if (indexResult.hasFailure()) {
                        response = null;
                    } else {
                        // update the version on request so it will happen on the replicas
                        final long version = indexResult.getVersion();
                        indexRequest.version(version);
                        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
                        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());
                        response = new IndexResponse(primary.shardId(), indexRequest.type(), indexRequest.id(),
                                indexResult.getVersion(), indexResult.isCreated());
                    }
                    operationResult = indexResult;
                    replicaRequest = request.items()[requestIndex];
                    break;
                case UPDATE:
                    UpdateResultHolder updateResultHolder = executeUpdateRequest(((UpdateRequest) itemRequest),
                            primary, metaData, request, requestIndex);
                    operationResult = updateResultHolder.operationResult;
                    response = updateResultHolder.response;
                    replicaRequest = updateResultHolder.replicaRequest;
                    break;
                case DELETE:
                    final DeleteRequest deleteRequest = (DeleteRequest) itemRequest;
                    Engine.DeleteResult deleteResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
                    if (deleteResult.hasFailure()) {
                        response = null;
                    } else {
                        // update the request with the version so it will go to the replicas
                        deleteRequest.versionType(deleteRequest.versionType().versionTypeForReplicationAndRecovery());
                        deleteRequest.version(deleteResult.getVersion());
                        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());
                        response = new DeleteResponse(request.shardId(), deleteRequest.type(), deleteRequest.id(),
                                deleteResult.getVersion(), deleteResult.isFound());
                    }
                    operationResult = deleteResult;
                    replicaRequest = request.items()[requestIndex];
                    break;
                default:
                    throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
            }
            // update the bulk item request because update request execution can mutate the bulk item request
            request.items()[requestIndex] = replicaRequest;
            if (operationResult == null) { // in case of noop update operation
                assert response.getResult() == DocWriteResponse.Result.NOOP
                        : "only noop update can have null operation";
                replicaRequest.setIgnoreOnReplica();
                replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), opType, response));
            } else if (operationResult.hasFailure() == false) {
                location = locationToSync(location, operationResult.getTranslogLocation());
                BulkItemResponse primaryResponse = new BulkItemResponse(replicaRequest.id(), opType, response);
                replicaRequest.setPrimaryResponse(primaryResponse);
                // set an empty ShardInfo to indicate no shards participated in the request execution
                // so we can safely send it to the replicas. We won't use it in the real response though.
                primaryResponse.getResponse().setShardInfo(new ShardInfo());
            } else {
                DocWriteRequest docWriteRequest = replicaRequest.request();
                Exception failure = operationResult.getFailure();
                if (isConflictException(failure)) {
                    logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                            request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
                } else {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                            request.shardId(), docWriteRequest.opType().getLowercase(), request), failure);
                }
                // if its a conflict failure, and we already executed the request on a primary (and we execute it
                // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
                // then just use the response we got from the successful execution
                if (replicaRequest.getPrimaryResponse() == null || isConflictException(failure) == false) {
                    replicaRequest.setIgnoreOnReplica();
                    replicaRequest.setPrimaryResponse(new BulkItemResponse(replicaRequest.id(), docWriteRequest.opType(),
                            new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), failure)));
                }
            }
            assert replicaRequest.getPrimaryResponse() != null;
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

    private static class UpdateResultHolder {
        final BulkItemRequest replicaRequest;
        final Engine.Result operationResult;
        final DocWriteResponse response;

        private UpdateResultHolder(BulkItemRequest replicaRequest, Engine.Result operationResult,
                                   DocWriteResponse response) {
            this.replicaRequest = replicaRequest;
            this.operationResult = operationResult;
            this.response = response;
        }
    }

    /**
     * Executes update request, delegating to a index or delete operation after translation,
     * handles retries on version conflict and constructs update response
     * NOTE: reassigns bulk item request at <code>requestIndex</code> for replicas to
     * execute translated update request (NOOP update is an exception). NOOP updates are
     * indicated by returning a <code>null</code> operation in {@link UpdateResultHolder}
     * */
    private UpdateResultHolder executeUpdateRequest(UpdateRequest updateRequest, IndexShard primary,
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
                updateOperationResult = new Engine.IndexResult(failure, updateRequest.version());
                break; // out of retry loop
            }
            // execute translated update request
            switch (translate.getResponseResult()) {
                case CREATED:
                case UPDATED:
                    IndexRequest indexRequest = translate.action();
                    MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                    indexRequest.process(mappingMd, allowIdGeneration, request.index());
                    updateOperationResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    if (updateOperationResult.hasFailure() == false) {
                        // update the version on request so it will happen on the replicas
                        final long version = updateOperationResult.getVersion();
                        indexRequest.version(version);
                        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
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
                                updateIndexRequest.type(), updateIndexRequest.id(),
                                updateOperationResult.getVersion(), ((Engine.IndexResult) updateOperationResult).isCreated());
                        BytesReference indexSourceAsBytes = updateIndexRequest.source();
                        updateResponse = new UpdateResponse(indexResponse.getShardInfo(),
                                indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(),
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
                                updateDeleteRequest.type(), updateDeleteRequest.id(),
                                updateOperationResult.getVersion(), ((Engine.DeleteResult) updateOperationResult).isFound());
                        updateResponse = new UpdateResponse(deleteResponse.getShardInfo(),
                                deleteResponse.getShardId(), deleteResponse.getType(), deleteResponse.getId(),
                                deleteResponse.getVersion(), deleteResponse.getResult());
                        updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest,
                                request.index(), deleteResponse.getVersion(), translate.updatedSourceAsMap(),
                                translate.updateSourceContentType(), null));
                        // set translated request as replica request
                        replicaRequest = new BulkItemRequest(request.items()[requestIndex].id(), updateDeleteRequest);
                        break;
                    default: throw new IllegalStateException("Illegal update operation " +
                            updateOperationResult.getOperationType().getLowercase());
                }
                // successful operation
                break; // out of retry loop
            } else if (updateOperationResult.getFailure() instanceof VersionConflictEngineException == false) {
                // not a version conflict exception
                break; // out of retry loop
            }
        }
        return new UpdateResultHolder(replicaRequest, updateOperationResult, updateResponse);
    }

    @Override
    public WriteReplicaResult<BulkShardRequest> shardOperationOnReplica(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.isIgnoreOnReplica() == false) {
                DocWriteRequest docWriteRequest = item.request();
                final Engine.Result operationResult;
                try {
                    switch (docWriteRequest.opType()) {
                        case CREATE:
                        case INDEX:
                            operationResult = executeIndexRequestOnReplica(((IndexRequest) docWriteRequest), replica);
                            break;
                        case DELETE:
                            operationResult = executeDeleteRequestOnReplica(((DeleteRequest) docWriteRequest), replica);
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
                                : "expected version conflict or mapper parsing failures";
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
                request.getContentType()).routing(request.routing()).parent(request.parent())
                .timestamp(request.timestamp()).ttl(request.ttl());

        final Engine.Index operation;
        try {
            operation = replica.prepareIndexOnReplica(sourceToParse, request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
        } catch (MapperParsingException e) {
            return new Engine.IndexResult(e, request.version());
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
                request.getContentType()).routing(request.routing()).parent(request.parent())
                .timestamp(request.timestamp()).ttl(request.ttl());
        return primary.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
    }

    /** Executes index operation on primary shard after updates mapping if dynamic mappings are found */
    public static Engine.IndexResult executeIndexRequestOnPrimary(IndexRequest request, IndexShard primary,
                                                                  MappingUpdatedAction mappingUpdatedAction) throws Exception {
        Engine.Index operation;
        try {
            operation = prepareIndexOperationOnPrimary(request, primary);
        } catch (MapperParsingException | IllegalArgumentException e) {
            return new Engine.IndexResult(e, request.version());
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
                return new Engine.IndexResult(e, request.version());
            }
            try {
                operation = prepareIndexOperationOnPrimary(request, primary);
            } catch (MapperParsingException | IllegalArgumentException e) {
                return new Engine.IndexResult(e, request.version());
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
                request.version(), request.versionType());
        return replica.delete(delete);
    }
}
