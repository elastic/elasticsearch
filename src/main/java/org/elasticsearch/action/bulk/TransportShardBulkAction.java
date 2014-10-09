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

import com.google.common.collect.Sets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;
import java.util.Set;

/**
 * Performs the index operation.
 */
public class TransportShardBulkAction extends TransportShardReplicationOperationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    private final static String OP_TYPE_UPDATE = "update";
    private final static String OP_TYPE_DELETE = "delete";

    private static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final MappingUpdatedAction mappingUpdatedAction;
    private final UpdateHelper updateHelper;
    private final boolean allowIdGeneration;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.updateHelper = updateHelper;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.BULK;
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected TransportRequestOptions transportOptions() {
        return BulkAction.INSTANCE.transportOptions(settings);
    }

    @Override
    protected BulkShardRequest newRequestInstance() {
        return new BulkShardRequest();
    }

    @Override
    protected BulkShardRequest newReplicaRequestInstance() {
        return new BulkShardRequest();
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
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        return clusterState.routingTable().index(request.concreteIndex()).shard(request.request().shardId()).shardsIt();
    }

    @Override
    protected PrimaryResponse<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final BulkShardRequest request = shardRequest.request;
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());
        Engine.IndexingOperation[] ops = null;
        final Set<String> mappingTypesToUpdate = Sets.newHashSet();

        long[] preVersions = new long[request.items().length];
        VersionType[] preVersionTypes = new VersionType[request.items().length];
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            BulkItemRequest item = request.items()[requestIndex];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                preVersions[requestIndex] = indexRequest.version();
                preVersionTypes[requestIndex] = indexRequest.versionType();
                try {
                    try {
                        WriteResult result = shardIndexOperation(request, indexRequest, clusterState, indexShard, true);
                        // add the response
                        IndexResponse indexResponse = result.response();
                        setResponse(item, new BulkItemResponse(item.id(), indexRequest.opType().lowercase(), indexResponse));
                        if (result.mappingTypeToUpdate != null) {
                            mappingTypesToUpdate.add(result.mappingTypeToUpdate);
                        }
                        if (result.op != null) {
                            if (ops == null) {
                                ops = new Engine.IndexingOperation[request.items().length];
                            }
                            ops[requestIndex] = result.op;
                        }
                    } catch (WriteFailure e) {
                        if (e.mappingTypeToUpdate != null) {
                            mappingTypesToUpdate.add(e.mappingTypeToUpdate);
                        }
                        throw e.getCause();
                    }
                } catch (Throwable e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < requestIndex; j++) {
                            applyVersion(request.items()[j], preVersions[j], preVersionTypes[j]);
                        }
                        for (String mappingTypeToUpdate : mappingTypesToUpdate) {
                            DocumentMapper docMapper = indexService.mapperService().documentMapper(mappingTypeToUpdate);
                            if (docMapper != null) {
                                mappingUpdatedAction.updateMappingOnMaster(indexService.index().name(), docMapper, indexService.indexUUID());
                            }
                        }
                        throw (ElasticsearchException) e;
                    }
                    if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("{} failed to execute bulk item (index) {}", e, shardRequest.shardId, indexRequest);
                    } else {
                        logger.debug("{} failed to execute bulk item (index) {}", e, shardRequest.shardId, indexRequest);
                    }
                    // if its a conflict failure, and we already executed the request on a primary (and we execute it
                    // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
                    // then just use the response we got from the successful execution
                    if (item.getPrimaryResponse() != null && isConflictException(e)) {
                        setResponse(item, item.getPrimaryResponse());
                    } else {
                        setResponse(item, new BulkItemResponse(item.id(), indexRequest.opType().lowercase(),
                                new BulkItemResponse.Failure(request.index(), indexRequest.type(), indexRequest.id(), e)));
                    }
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                preVersions[requestIndex] = deleteRequest.version();
                preVersionTypes[requestIndex] = deleteRequest.versionType();

                try {
                    // add the response
                    DeleteResponse deleteResponse = shardDeleteOperation(request, deleteRequest, indexShard).response();
                    setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_DELETE, deleteResponse));
                } catch (Throwable e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < requestIndex; j++) {
                            applyVersion(request.items()[j], preVersions[j], preVersionTypes[j]);
                        }
                        throw (ElasticsearchException) e;
                    }
                    if (e instanceof ElasticsearchException && ((ElasticsearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("{} failed to execute bulk item (delete) {}", e, shardRequest.shardId, deleteRequest);
                    } else {
                        logger.debug("{} failed to execute bulk item (delete) {}", e, shardRequest.shardId, deleteRequest);
                    }
                    // if its a conflict failure, and we already executed the request on a primary (and we execute it
                    // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
                    // then just use the response we got from the successful execution
                    if (item.getPrimaryResponse() != null && isConflictException(e)) {
                        setResponse(item, item.getPrimaryResponse());
                    } else {
                        setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_DELETE,
                                new BulkItemResponse.Failure(request.index(), deleteRequest.type(), deleteRequest.id(), e)));
                    }
                }
            } else if (item.request() instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) item.request();
                preVersions[requestIndex] = updateRequest.version();
                preVersionTypes[requestIndex] = updateRequest.versionType();
                //  We need to do the requested retries plus the initial attempt. We don't do < 1+retry_on_conflict because retry_on_conflict may be Integer.MAX_VALUE
                for (int updateAttemptsCount = 0; updateAttemptsCount <= updateRequest.retryOnConflict(); updateAttemptsCount++) {
                    UpdateResult updateResult;
                    try {
                        updateResult = shardUpdateOperation(clusterState, request, updateRequest, indexShard);
                    } catch (Throwable t) {
                        updateResult = new UpdateResult(null, null, false, t, null);
                    }
                    if (updateResult.success()) {

                        switch (updateResult.result.operation()) {
                            case UPSERT:
                            case INDEX:
                                WriteResult result = updateResult.writeResult;
                                IndexRequest indexRequest = updateResult.request();
                                BytesReference indexSourceAsBytes = indexRequest.source();
                                // add the response
                                IndexResponse indexResponse = result.response();
                                UpdateResponse updateResponse = new UpdateResponse(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId(), indexResponse.getVersion(), indexResponse.isCreated());
                                if (updateRequest.fields() != null && updateRequest.fields().length > 0) {
                                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                                    updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, shardRequest.request.index(), indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                                }
                                item = request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), indexRequest);
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE, updateResponse));
                                if (result.mappingTypeToUpdate != null) {
                                    mappingTypesToUpdate.add(result.mappingTypeToUpdate);
                                }
                                if (result.op != null) {
                                    if (ops == null) {
                                        ops = new Engine.IndexingOperation[request.items().length];
                                    }
                                    ops[requestIndex] = result.op;
                                }
                                // Replace the update request to the translated index request to execute on the replica.
                                break;
                            case DELETE:
                                DeleteResponse response = updateResult.writeResult.response();
                                DeleteRequest deleteRequest = updateResult.request();
                                updateResponse = new UpdateResponse(response.getIndex(), response.getType(), response.getId(), response.getVersion(), false);
                                updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, shardRequest.request.index(), response.getVersion(), updateResult.result.updatedSourceAsMap(), updateResult.result.updateSourceContentType(), null));
                                // Replace the update request to the translated delete request to execute on the replica.
                                item = request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), deleteRequest);
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE, updateResponse));
                                break;
                            case NONE:
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE, updateResult.noopResult));
                                item.setIgnoreOnReplica(); // no need to go to the replica
                                break;
                        }
                        // NOTE: Breaking out of the retry_on_conflict loop!
                        break;
                    } else if (updateResult.failure()) {
                        Throwable t = updateResult.error;
                        if (updateResult.retry) {
                            // updateAttemptCount is 0 based and marks current attempt, if it's equal to retryOnConflict we are going out of the iteration
                            if (updateAttemptsCount >= updateRequest.retryOnConflict()) {
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE,
                                        new BulkItemResponse.Failure(request.index(), updateRequest.type(), updateRequest.id(), t)));
                            }
                        } else {
                            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                            if (retryPrimaryException(t)) {
                                // restore updated versions...
                                for (int j = 0; j < requestIndex; j++) {
                                    applyVersion(request.items()[j], preVersions[j], preVersionTypes[j]);
                                }
                                throw (ElasticsearchException) t;
                            }
                            // if its a conflict failure, and we already executed the request on a primary (and we execute it
                            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
                            // then just use the response we got from the successful execution
                            if (item.getPrimaryResponse() != null && isConflictException(t)) {
                                setResponse(item, item.getPrimaryResponse());
                            } else if (updateResult.result == null) {
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE, new BulkItemResponse.Failure(shardRequest.request.index(), updateRequest.type(), updateRequest.id(), t)));
                            } else {
                                switch (updateResult.result.operation()) {
                                    case UPSERT:
                                    case INDEX:
                                        IndexRequest indexRequest = updateResult.request();
                                        if (t instanceof ElasticsearchException && ((ElasticsearchException) t).status() == RestStatus.CONFLICT) {
                                            logger.trace("{} failed to execute bulk item (index) {}", t, shardRequest.shardId, indexRequest);
                                        } else {
                                            logger.debug("{} failed to execute bulk item (index) {}", t, shardRequest.shardId, indexRequest);
                                        }
                                        setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE,
                                                new BulkItemResponse.Failure(request.index(), indexRequest.type(), indexRequest.id(), t)));
                                        break;
                                    case DELETE:
                                        DeleteRequest deleteRequest = updateResult.request();
                                        if (t instanceof ElasticsearchException && ((ElasticsearchException) t).status() == RestStatus.CONFLICT) {
                                            logger.trace("{} failed to execute bulk item (delete) {}", t, shardRequest.shardId, deleteRequest);
                                        } else {
                                            logger.debug("{} failed to execute bulk item (delete) {}", t, shardRequest.shardId, deleteRequest);
                                        }
                                        setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_DELETE,
                                                new BulkItemResponse.Failure(request.index(), deleteRequest.type(), deleteRequest.id(), t)));
                                        break;
                                }
                            }
                            // NOTE: Breaking out of the retry_on_conflict loop!
                            break;
                        }

                    }
                }
            }

            assert item.getPrimaryResponse() != null;
            assert preVersionTypes[requestIndex] != null;
        }

        for (String mappingTypToUpdate : mappingTypesToUpdate) {
            DocumentMapper docMapper = indexService.mapperService().documentMapper(mappingTypToUpdate);
            if (docMapper != null) {
                mappingUpdatedAction.updateMappingOnMaster(indexService.index().name(), docMapper, indexService.indexUUID());
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_bulk").force(false));
            } catch (Throwable e) {
                // ignore
            }
        }
        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        BulkShardResponse response = new BulkShardResponse(shardRequest.shardId, responses);
        return new PrimaryResponse<>(shardRequest.request, response, ops);
    }

    private void setResponse(BulkItemRequest request, BulkItemResponse response) {
        request.setPrimaryResponse(response);
        if (response.isFailed()) {
            request.setIgnoreOnReplica();
        }
    }

    static class WriteResult {

        final Object response;
        final String mappingTypeToUpdate;
        final Engine.IndexingOperation op;

        WriteResult(Object response, String mappingTypeToUpdate, Engine.IndexingOperation op) {
            this.response = response;
            this.mappingTypeToUpdate = mappingTypeToUpdate;
            this.op = op;
        }

        @SuppressWarnings("unchecked")
        <T> T response() {
            return (T) response;
        }

    }

    static class WriteFailure extends ElasticsearchException implements ElasticsearchWrapperException {
        @Nullable
        final String mappingTypeToUpdate;

        WriteFailure(Throwable cause, String mappingTypeToUpdate) {
            super(null, cause);
            assert cause != null;
            this.mappingTypeToUpdate = mappingTypeToUpdate;
        }
    }

    private WriteResult shardIndexOperation(BulkShardRequest request, IndexRequest indexRequest, ClusterState clusterState,
                                            IndexShard indexShard, boolean processed) {

        // validate, if routing is required, that we got routing
        MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (indexRequest.routing() == null) {
                throw new RoutingMissingException(request.index(), indexRequest.type(), indexRequest.id());
            }
        }

        if (!processed) {
            indexRequest.process(clusterState.metaData(), mappingMd, allowIdGeneration, request.index());
        }

        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

        // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
        String mappingTypeToUpdate = null;

        long version;
        boolean created;
        Engine.IndexingOperation op;
        try {
            if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                Engine.Index index = indexShard.prepareIndex(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.PRIMARY, request.canHaveDuplicates() || indexRequest.canHaveDuplicates());
                if (index.parsedDoc().mappingsModified()) {
                    mappingTypeToUpdate = indexRequest.type();
                }
                indexShard.index(index);
                version = index.version();
                op = index;
                created = index.created();
            } else {
                Engine.Create create = indexShard.prepareCreate(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.PRIMARY,
                        request.canHaveDuplicates() || indexRequest.canHaveDuplicates(), indexRequest.autoGeneratedId());
                if (create.parsedDoc().mappingsModified()) {
                    mappingTypeToUpdate = indexRequest.type();
                }
                indexShard.create(create);
                version = create.version();
                op = create;
                created = true;
            }
            // update the version on request so it will happen on the replicas
            indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
            indexRequest.version(version);
        } catch (Throwable t) {
            throw new WriteFailure(t, mappingTypeToUpdate);
        }

        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());


        IndexResponse indexResponse = new IndexResponse(request.index(), indexRequest.type(), indexRequest.id(), version, created);
        return new WriteResult(indexResponse, mappingTypeToUpdate, op);
    }

    private WriteResult shardDeleteOperation(BulkShardRequest request, DeleteRequest deleteRequest, IndexShard indexShard) {
        Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version(), deleteRequest.versionType(), Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        deleteRequest.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        deleteRequest.version(delete.version());

        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());

        DeleteResponse deleteResponse = new DeleteResponse(request.index(), deleteRequest.type(), deleteRequest.id(), delete.version(), delete.found());
        return new WriteResult(deleteResponse, null, null);
    }

    static class UpdateResult {

        final UpdateHelper.Result result;
        final ActionRequest actionRequest;
        final boolean retry;
        final Throwable error;
        final WriteResult writeResult;
        final UpdateResponse noopResult;

        UpdateResult(UpdateHelper.Result result, ActionRequest actionRequest, boolean retry, Throwable error, WriteResult writeResult) {
            this.result = result;
            this.actionRequest = actionRequest;
            this.retry = retry;
            this.error = error;
            this.writeResult = writeResult;
            this.noopResult = null;
        }

        UpdateResult(UpdateHelper.Result result, ActionRequest actionRequest, WriteResult writeResult) {
            this.result = result;
            this.actionRequest = actionRequest;
            this.writeResult = writeResult;
            this.retry = false;
            this.error = null;
            this.noopResult = null;
        }

        public UpdateResult(UpdateHelper.Result result, UpdateResponse updateResponse) {
            this.result = result;
            this.noopResult = updateResponse;
            this.actionRequest = null;
            this.writeResult = null;
            this.retry = false;
            this.error = null;
        }


        boolean failure() {
            return error != null;
        }

        boolean success() {
            return noopResult != null || writeResult != null;
        }

        @SuppressWarnings("unchecked")
        <T extends ActionRequest> T request() {
            return (T) actionRequest;
        }


    }

    private UpdateResult shardUpdateOperation(ClusterState clusterState, BulkShardRequest bulkShardRequest, UpdateRequest updateRequest, IndexShard indexShard) {
        UpdateHelper.Result translate = updateHelper.prepare(updateRequest, indexShard);
        switch (translate.operation()) {
            case UPSERT:
            case INDEX:
                IndexRequest indexRequest = translate.action();
                try {
                    WriteResult result = shardIndexOperation(bulkShardRequest, indexRequest, clusterState, indexShard, false);
                    return new UpdateResult(translate, indexRequest, result);
                } catch (Throwable t) {
                    t = ExceptionsHelper.unwrapCause(t);
                    boolean retry = false;
                    if (t instanceof VersionConflictEngineException || (t instanceof DocumentAlreadyExistsException && translate.operation() == UpdateHelper.Operation.UPSERT)) {
                        retry = true;
                    }
                    return new UpdateResult(translate, indexRequest, retry, t, null);
                }
            case DELETE:
                DeleteRequest deleteRequest = translate.action();
                try {
                    WriteResult result = shardDeleteOperation(bulkShardRequest, deleteRequest, indexShard);
                    return new UpdateResult(translate, deleteRequest, result);
                } catch (Throwable t) {
                    t = ExceptionsHelper.unwrapCause(t);
                    boolean retry = false;
                    if (t instanceof VersionConflictEngineException) {
                        retry = true;
                    }
                    return new UpdateResult(translate, deleteRequest, retry, t, null);
                }
            case NONE:
                UpdateResponse updateResponse = translate.action();
                indexShard.indexingService().noopUpdate(updateRequest.type());
                return new UpdateResult(translate, updateResponse);
            default:
                throw new ElasticsearchIllegalStateException("Illegal update operation " + translate.operation());
        }
    }


    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        final BulkShardRequest request = shardRequest.request;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null || item.isIgnoreOnReplica()) {
                continue;
            }
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

                    if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                        Engine.Index index = indexShard.prepareIndex(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.REPLICA, request.canHaveDuplicates() || indexRequest.canHaveDuplicates());
                        indexShard.index(index);
                    } else {
                        Engine.Create create = indexShard.prepareCreate(sourceToParse,
                                indexRequest.version(), indexRequest.versionType(),
                                Engine.Operation.Origin.REPLICA, request.canHaveDuplicates() || indexRequest.canHaveDuplicates(), indexRequest.autoGeneratedId());
                        indexShard.create(create);
                    }
                } catch (Throwable e) {
                    // if its not an ignore replica failure, we need to make sure to bubble up the failure
                    // so we will fail the shard
                    if (!ignoreReplicaException(e)) {
                        throw e;
                    }
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version(), deleteRequest.versionType(), Engine.Operation.Origin.REPLICA);
                    indexShard.delete(delete);
                } catch (Throwable e) {
                    // if its not an ignore replica failure, we need to make sure to bubble up the failure
                    // so we will fail the shard
                    if (!ignoreReplicaException(e)) {
                        throw e;
                    }
                }
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_bulk").force(false));
            } catch (Throwable e) {
                // ignore
            }
        }
    }

    private void applyVersion(BulkItemRequest item, long version, VersionType versionType) {
        if (item.request() instanceof IndexRequest) {
            ((IndexRequest) item.request()).version(version).versionType(versionType);
        } else if (item.request() instanceof DeleteRequest) {
            ((DeleteRequest) item.request()).version(version).versionType();
        } else if (item.request() instanceof UpdateRequest) {
            ((UpdateRequest) item.request()).version(version).versionType();
        } else {
            // log?
        }
    }
}
