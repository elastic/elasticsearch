/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.percolator.PercolatorExecutor;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.ShardId;
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

    private final MappingUpdatedAction mappingUpdatedAction;
    private final UpdateHelper updateHelper;
    private final boolean allowIdGeneration;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, UpdateHelper updateHelper) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
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
    protected String transportAction() {
        return BulkAction.NAME + "/shard";
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, BulkShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, BulkShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, BulkShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }

    @Override
    protected PrimaryResponse<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final BulkShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        Engine.IndexingOperation[] ops = null;
        Set<Tuple<String, String>> mappingsToUpdate = null;

        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        long[] preVersions = new long[request.items().length];
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            BulkItemRequest item = request.items()[requestIndex];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    WriteResult result = shardIndexOperation(request, indexRequest, clusterState, indexShard, true);
                    // add the response
                    IndexResponse indexResponse = result.response();
                    responses[requestIndex] = new BulkItemResponse(item.id(), indexRequest.opType().lowercase(), indexResponse);
                    preVersions[requestIndex] = result.preVersion;
                    if (result.mappingToUpdate != null) {
                        if (mappingsToUpdate == null) {
                            mappingsToUpdate = Sets.newHashSet();
                        }
                        mappingsToUpdate.add(result.mappingToUpdate);
                    }
                    if (result.op != null) {
                        if (ops == null) {
                            ops = new Engine.IndexingOperation[request.items().length];
                        }
                        ops[requestIndex] = result.op;
                    }
                } catch (Throwable e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < requestIndex; j++) {
                            applyVersion(request.items()[j], preVersions[j]);
                        }
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    }
                    responses[requestIndex] = new BulkItemResponse(item.id(), indexRequest.opType().lowercase(),
                            new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items()[requestIndex] = null;
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    // add the response
                    DeleteResponse deleteResponse = shardDeleteOperation(deleteRequest, indexShard).response();
                    responses[requestIndex] = new BulkItemResponse(item.id(), "delete", deleteResponse);
                } catch (Throwable e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < requestIndex; j++) {
                            applyVersion(request.items()[j], preVersions[j]);
                        }
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    }
                    responses[requestIndex] = new BulkItemResponse(item.id(), "delete",
                            new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items()[requestIndex] = null;
                }
            } else if (item.request() instanceof UpdateRequest) {
                UpdateRequest updateRequest = (UpdateRequest) item.request();
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
                                UpdateResponse updateResponse = new UpdateResponse(indexResponse.getIndex(), indexResponse.getType(), indexResponse.getId(), indexResponse.getVersion());
                                updateResponse.setMatches(indexResponse.getMatches());
                                if (updateRequest.fields() != null && updateRequest.fields().length > 0) {
                                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                                    updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                                }
                                responses[requestIndex] = new BulkItemResponse(item.id(), "update", updateResponse);
                                preVersions[requestIndex] = result.preVersion;
                                if (result.mappingToUpdate != null) {
                                    if (mappingsToUpdate == null) {
                                        mappingsToUpdate = Sets.newHashSet();
                                    }
                                    mappingsToUpdate.add(result.mappingToUpdate);
                                }
                                if (result.op != null) {
                                    if (ops == null) {
                                        ops = new Engine.IndexingOperation[request.items().length];
                                    }
                                    ops[requestIndex] = result.op;
                                }
                                // Replace the update request to the translated index request to execute on the replica.
                                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), indexRequest);
                                break;
                            case DELETE:
                                DeleteResponse response = updateResult.writeResult.response();
                                DeleteRequest deleteRequest = updateResult.request();
                                updateResponse = new UpdateResponse(response.getIndex(), response.getType(), response.getId(), response.getVersion());
                                updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, response.getVersion(), updateResult.result.updatedSourceAsMap(), updateResult.result.updateSourceContentType(), null));
                                responses[requestIndex] = new BulkItemResponse(item.id(), "update", updateResponse);
                                // Replace the update request to the translated delete request to execute on the replica.
                                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), deleteRequest);
                                break;
                            case NONE:
                                responses[requestIndex] = new BulkItemResponse(item.id(), "update", updateResult.noopResult);
                                request.items()[requestIndex] = null; // No need to go to the replica
                                break;
                        }
                        // NOTE: Breaking out of the retry_on_conflict loop!
                        break;
                    } else if (updateResult.failure()) {
                        Throwable t = updateResult.error;
                        if (updateResult.retry) {
                            // updateAttemptCount is 0 based and marks current attempt, if it's equal to retryOnConflict we are going out of the iteration
                            if (updateAttemptsCount >= updateRequest.retryOnConflict()) {
                                // we can't try any more
                                responses[requestIndex] = new BulkItemResponse(item.id(), "update",
                                        new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(), updateRequest.id(), ExceptionsHelper.detailedMessage(t)));
                                ;

                                request.items()[requestIndex] = null; // do not send to replicas
                            }
                        } else {
                            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                            if (retryPrimaryException(t)) {
                                // restore updated versions...
                                for (int j = 0; j < requestIndex; j++) {
                                    applyVersion(request.items()[j], preVersions[j]);
                                }
                                throw (ElasticSearchException) t;
                            }
                            if (updateResult.result == null) {
                                responses[requestIndex] = new BulkItemResponse(item.id(), "update", new BulkItemResponse.Failure(updateRequest.index(), updateRequest.type(), updateRequest.id(), ExceptionsHelper.detailedMessage(t)));
                            } else {
                                switch (updateResult.result.operation()) {
                                    case UPSERT:
                                    case INDEX:
                                        IndexRequest indexRequest = updateResult.request();
                                        if (t instanceof ElasticSearchException && ((ElasticSearchException) t).status() == RestStatus.CONFLICT) {
                                            logger.trace("[{}][{}] failed to execute bulk item (index) {}", t, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                                        } else {
                                            logger.debug("[{}][{}] failed to execute bulk item (index) {}", t, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                                        }
                                        responses[requestIndex] = new BulkItemResponse(item.id(), indexRequest.opType().lowercase(),
                                                new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), ExceptionsHelper.detailedMessage(t)));
                                        break;
                                    case DELETE:
                                        DeleteRequest deleteRequest = updateResult.request();
                                        if (t instanceof ElasticSearchException && ((ElasticSearchException) t).status() == RestStatus.CONFLICT) {
                                            logger.trace("[{}][{}] failed to execute bulk item (delete) {}", t, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                                        } else {
                                            logger.debug("[{}][{}] failed to execute bulk item (delete) {}", t, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                                        }
                                        responses[requestIndex] = new BulkItemResponse(item.id(), "delete",
                                                new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), ExceptionsHelper.detailedMessage(t)));
                                        break;
                                }
                            }
                            // nullify the request so it won't execute on the replicas
                            request.items()[requestIndex] = null;
                            // NOTE: Breaking out of the retry_on_conflict loop!
                            break;
                        }

                    }
                }
            }

            assert responses[requestIndex] != null; // we must have set a response somewhere.

        }

        if (mappingsToUpdate != null) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2());
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_bulk").force(false));
            } catch (Throwable e) {
                // ignore
            }
        }
        BulkShardResponse response = new BulkShardResponse(new ShardId(request.index(), request.shardId()), responses);
        return new PrimaryResponse<BulkShardResponse, BulkShardRequest>(shardRequest.request, response, ops);
    }

    static class WriteResult {

        final Object response;
        final long preVersion;
        final Tuple<String, String> mappingToUpdate;
        final Engine.IndexingOperation op;

        WriteResult(Object response, long preVersion, Tuple<String, String> mappingToUpdate, Engine.IndexingOperation op) {
            this.response = response;
            this.preVersion = preVersion;
            this.mappingToUpdate = mappingToUpdate;
            this.op = op;
        }

        @SuppressWarnings("unchecked")
        <T> T response() {
            return (T) response;
        }

    }

    private WriteResult shardIndexOperation(BulkShardRequest request, IndexRequest indexRequest, ClusterState clusterState,
                                            IndexShard indexShard, boolean processed) {

        // validate, if routing is required, that we got routing
        MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (indexRequest.routing() == null) {
                throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
            }
        }

        if (!processed) {
            indexRequest.process(clusterState.metaData(), indexRequest.index(), mappingMd, allowIdGeneration);
        }

        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

        long version;
        Engine.IndexingOperation op;
        if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).versionType(indexRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
            indexShard.index(index);
            version = index.version();
            op = index;
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).versionType(indexRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
            indexShard.create(create);
            version = create.version();
            op = create;
        }
        long preVersion = indexRequest.version();
        // update the version on request so it will happen on the replicas
        indexRequest.version(version);

        // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
        Tuple<String, String> mappingsToUpdate = null;
        if (op.parsedDoc().mappingsModified()) {
            mappingsToUpdate = Tuple.tuple(indexRequest.index(), indexRequest.type());
        }

        // if we are going to percolate, then we need to keep this op for the postPrimary operation
        if (!Strings.hasLength(indexRequest.percolate())) {
            op = null;
        }

        IndexResponse indexResponse = new IndexResponse(indexRequest.index(), indexRequest.type(), indexRequest.id(), version);
        return new WriteResult(indexResponse, preVersion, mappingsToUpdate, op);
    }

    private WriteResult shardDeleteOperation(DeleteRequest deleteRequest, IndexShard indexShard) {
        Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).versionType(deleteRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with the version so it will go to the replicas
        deleteRequest.version(delete.version());
        DeleteResponse deleteResponse = new DeleteResponse(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), delete.version(), delete.notFound());
        return new WriteResult(deleteResponse, deleteRequest.version(), null, null);
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
                    WriteResult result = shardDeleteOperation(deleteRequest, indexShard);
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
                return new UpdateResult(translate, updateResponse);
            default:
                throw new ElasticSearchIllegalStateException("Illegal update operation " + translate.operation());
        }
    }

    @Override
    protected void postPrimaryOperation(BulkShardRequest request, PrimaryResponse<BulkShardResponse, BulkShardRequest> response) {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        Engine.IndexingOperation[] ops = (Engine.IndexingOperation[]) response.payload();
        if (ops == null) {
            return;
        }
        for (int i = 0; i < ops.length; i++) {
            BulkItemRequest itemRequest = request.items()[i];
            BulkItemResponse itemResponse = response.response().getResponses()[i];
            if (itemResponse.isFailed()) {
                // failure, continue
                continue;
            }
            Engine.IndexingOperation op = ops[i];
            if (op == null) {
                continue; // failed / no matches requested
            }
            if (itemRequest.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) itemRequest.request();
                if (!Strings.hasLength(indexRequest.percolate())) {
                    continue;
                }
                try {
                    PercolatorExecutor.Response percolate = indexService.percolateService().percolate(new PercolatorExecutor.DocAndSourceQueryRequest(op.parsedDoc(), indexRequest.percolate()));
                    ((IndexResponse) itemResponse.getResponse()).setMatches(percolate.matches());
                } catch (Throwable e) {
                    logger.warn("failed to percolate [{}]", e, itemRequest.request());
                }
            }
        }
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        final BulkShardRequest request = shardRequest.request;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null) {
                continue;
            }
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

                    if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                        Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.index(index);
                    } else {
                        Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.create(create);
                    }
                } catch (Throwable e) {
                    // ignore, we are on backup
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.delete(delete);
                } catch (Throwable e) {
                    // ignore, we are on backup
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

    private void updateMappingOnMaster(final String index, final String type) {
        try {
            MapperService mapperService = indicesService.indexServiceSafe(index).mapperService();
            final DocumentMapper documentMapper = mapperService.documentMapper(type);
            if (documentMapper == null) { // should not happen
                return;
            }
            IndexMetaData metaData = clusterService.state().metaData().index(index);
            if (metaData == null) {
                return;
            }

            // we generate the order id before we get the mapping to send and refresh the source, so
            // if 2 happen concurrently, we know that the later order will include the previous one
            long orderId = mappingUpdatedAction.generateNextMappingUpdateOrder();
            documentMapper.refreshSource();

            DiscoveryNode node = clusterService.localNode();
            final MappingUpdatedAction.MappingUpdatedRequest request = new MappingUpdatedAction.MappingUpdatedRequest(index, metaData.uuid(), type, documentMapper.mappingSource(), orderId, node != null ? node.id() : null);
            mappingUpdatedAction.execute(request, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override
                public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.warn("failed to update master on updated mapping for {}", e, request);
                }
            });
        } catch (Throwable e) {
            logger.warn("failed to update master on updated mapping for index [{}], type [{}]", e, index, type);
        }
    }

    private void applyVersion(BulkItemRequest item, long version) {
        if (item.request() instanceof IndexRequest) {
            ((IndexRequest) item.request()).version(version);
        } else if (item.request() instanceof DeleteRequest) {
            ((DeleteRequest) item.request()).version(version);
        } else {
            // log?
        }
    }
}
