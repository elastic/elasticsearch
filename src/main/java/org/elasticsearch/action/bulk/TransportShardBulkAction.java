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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionWriteResponse;
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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.river.RiverIndexName;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

/**
 * Performs the index operation.
 */
public class TransportShardBulkAction extends TransportShardReplicationOperationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    private final static String OP_TYPE_UPDATE = "update";
    private final static String OP_TYPE_DELETE = "delete";

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final MappingUpdatedAction mappingUpdatedAction;
    private final UpdateHelper updateHelper;
    private final boolean allowIdGeneration;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                BulkShardRequest.class, BulkShardRequest.class, ThreadPool.Names.BULK);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.updateHelper = updateHelper;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
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
    protected Tuple<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final BulkShardRequest request = shardRequest.request;
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardRequest.shardId.id());

        long[] preVersions = new long[request.items().length];
        VersionType[] preVersionTypes = new VersionType[request.items().length];
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            BulkItemRequest item = request.items()[requestIndex];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                preVersions[requestIndex] = indexRequest.version();
                preVersionTypes[requestIndex] = indexRequest.versionType();
                try {
                    WriteResult result = shardIndexOperation(request, indexRequest, clusterState, indexShard, indexService, true);
                    // add the response
                    IndexResponse indexResponse = result.response();
                    setResponse(item, new BulkItemResponse(item.id(), indexRequest.opType().lowercase(), indexResponse));
                } catch (Throwable e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < requestIndex; j++) {
                            applyVersion(request.items()[j], preVersions[j], preVersionTypes[j]);
                        }
                        throw (ElasticsearchException) e;
                    }
                    if (ExceptionsHelper.status(e) == RestStatus.CONFLICT) {
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
                    if (ExceptionsHelper.status(e) == RestStatus.CONFLICT) {
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
                        updateResult = shardUpdateOperation(clusterState, request, updateRequest, indexShard, indexService);
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
                                UpdateResponse updateResponse = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId(), indexResponse.getVersion(), indexResponse.isCreated());
                                if (updateRequest.fields() != null && updateRequest.fields().length > 0) {
                                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                                    updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, shardRequest.request.index(), indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                                }
                                item = request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), indexRequest);
                                setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE, updateResponse));
                                break;
                            case DELETE:
                                DeleteResponse response = updateResult.writeResult.response();
                                DeleteRequest deleteRequest = updateResult.request();
                                updateResponse = new UpdateResponse(response.getShardInfo(), response.getIndex(), response.getType(), response.getId(), response.getVersion(), false);
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
                                        if (ExceptionsHelper.status(t) == RestStatus.CONFLICT) {
                                            logger.trace("{} failed to execute bulk item (index) {}", t, shardRequest.shardId, indexRequest);
                                        } else {
                                            logger.debug("{} failed to execute bulk item (index) {}", t, shardRequest.shardId, indexRequest);
                                        }
                                        setResponse(item, new BulkItemResponse(item.id(), OP_TYPE_UPDATE,
                                                new BulkItemResponse.Failure(request.index(), indexRequest.type(), indexRequest.id(), t)));
                                        break;
                                    case DELETE:
                                        DeleteRequest deleteRequest = updateResult.request();
                                        if (ExceptionsHelper.status(t) == RestStatus.CONFLICT) {
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
            } else {
                throw new ElasticsearchIllegalStateException("Unexpected index operation: " + item.request());
            }

            assert item.getPrimaryResponse() != null;
            assert preVersionTypes[requestIndex] != null;
        }

        if (request.refresh()) {
            try {
                indexShard.refresh("refresh_flag_bulk");
            } catch (Throwable e) {
                // ignore
            }
        }
        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        return new Tuple<>(new BulkShardResponse(shardRequest.shardId, responses), shardRequest.request);
    }

    private void setResponse(BulkItemRequest request, BulkItemResponse response) {
        request.setPrimaryResponse(response);
        if (response.isFailed()) {
            request.setIgnoreOnReplica();
        }
    }

    static class WriteResult {

        final ActionWriteResponse response;
        final Engine.IndexingOperation op;

        WriteResult(ActionWriteResponse response, Engine.IndexingOperation op) {
            this.response = response;
            this.op = op;
        }

        @SuppressWarnings("unchecked")
        <T extends ActionWriteResponse> T response() {
            // this sets total, pending and failed to 0 and this is ok, because we will embed this into the replica
            // request and not use it
            response.setShardInfo(new ActionWriteResponse.ShardInfo());
            return (T) response;
        }

    }

    private WriteResult shardIndexOperation(BulkShardRequest request, IndexRequest indexRequest, ClusterState clusterState,
                                            IndexShard indexShard, IndexService indexService, boolean processed) throws Throwable {

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

        final Engine.IndexingOperation operation;
        if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
            operation = indexShard.prepareIndex(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.PRIMARY, request.canHaveDuplicates() || indexRequest.canHaveDuplicates());
        } else {
            assert indexRequest.opType() == IndexRequest.OpType.CREATE : indexRequest.opType();
            operation = indexShard.prepareCreate(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.PRIMARY,
                    request.canHaveDuplicates() || indexRequest.canHaveDuplicates(), indexRequest.autoGeneratedId());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final boolean created;
        if (update != null) {
            final String indexName = indexService.index().name();
            if (indexName.equals(RiverIndexName.Conf.indexName(settings))) {
                // With rivers, we have a chicken and egg problem if indexing
                // the _meta document triggers a mapping update. Because we would
                // like to validate the mapping update first, but on the other
                // hand putting the mapping would start the river, which expects
                // to find a _meta document
                // So we have no choice but to index first and send mappings afterwards
                MapperService mapperService = indexService.mapperService();
                mapperService.merge(indexRequest.type(), new CompressedString(update.toBytes()), true);
                created = operation.execute(indexShard);
                mappingUpdatedAction.updateMappingOnMasterAsynchronously(indexName, indexRequest.type(), update);
            } else {
                mappingUpdatedAction.updateMappingOnMasterSynchronously(indexName, indexRequest.type(), update);
                created = operation.execute(indexShard);
            }
        } else {
            created = operation.execute(indexShard);
        }

        // update the version on request so it will happen on the replicas
        final long version = operation.version();
        indexRequest.versionType(indexRequest.versionType().versionTypeForReplicationAndRecovery());
        indexRequest.version(version);

        assert indexRequest.versionType().validateVersionForWrites(indexRequest.version());

        IndexResponse indexResponse = new IndexResponse(request.index(), indexRequest.type(), indexRequest.id(), version, created);
        return new WriteResult(indexResponse, operation);
    }

    private WriteResult shardDeleteOperation(BulkShardRequest request, DeleteRequest deleteRequest, IndexShard indexShard) {
        Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version(), deleteRequest.versionType(), Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        deleteRequest.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        deleteRequest.version(delete.version());

        assert deleteRequest.versionType().validateVersionForWrites(deleteRequest.version());

        DeleteResponse deleteResponse = new DeleteResponse(request.index(), deleteRequest.type(), deleteRequest.id(), delete.version(), delete.found());
        return new WriteResult(deleteResponse, null);
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

    private UpdateResult shardUpdateOperation(ClusterState clusterState, BulkShardRequest bulkShardRequest, UpdateRequest updateRequest, IndexShard indexShard, IndexService indexService) {
        UpdateHelper.Result translate = updateHelper.prepare(updateRequest, indexShard);
        switch (translate.operation()) {
            case UPSERT:
            case INDEX:
                IndexRequest indexRequest = translate.action();
                try {
                    WriteResult result = shardIndexOperation(bulkShardRequest, indexRequest, clusterState, indexShard, indexService, false);
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


    @Override
    protected void shardOperationOnReplica(ShardId shardId, BulkShardRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
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

                    final Engine.IndexingOperation operation;
                    if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                        operation = indexShard.prepareIndex(sourceToParse, indexRequest.version(), indexRequest.versionType(), Engine.Operation.Origin.REPLICA, request.canHaveDuplicates() || indexRequest.canHaveDuplicates());
                    } else {
                        assert indexRequest.opType() == IndexRequest.OpType.CREATE : indexRequest.opType();
                        operation = indexShard.prepareCreate(sourceToParse,
                                indexRequest.version(), indexRequest.versionType(),
                                Engine.Operation.Origin.REPLICA, request.canHaveDuplicates() || indexRequest.canHaveDuplicates(), indexRequest.autoGeneratedId());
                    }
                    Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
                    if (update != null) {
                        throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
                    }
                    operation.execute(indexShard);
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
            } else {
                throw new ElasticsearchIllegalStateException("Unexpected index operation: " + item.request());
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh("refresh_flag_bulk");
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
