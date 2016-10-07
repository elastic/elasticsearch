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

package org.elasticsearch.action.update;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.ExceptionsHelper.unwrapCause;

/**
 */
public class TransportUpdateAction extends TransportWriteAction<UpdateRequest, UpdateReplicaRequest, UpdateResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;
    private final UpdateHelper updateHelper;
    private final IndicesService indicesService;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final boolean allowIdGeneration;

    @Inject
    public TransportUpdateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                 TransportCreateIndexAction createIndexAction, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver, IndicesService indicesService,
                                 AutoCreateIndex autoCreateIndex, ShardStateAction shardStateAction,
                                 MappingUpdatedAction mappingUpdatedAction, ScriptService scriptService) {
        super(settings, UpdateAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
                actionFilters, indexNameExpressionResolver, UpdateRequest::new, UpdateReplicaRequest::new, ThreadPool.Names.INDEX);
        this.createIndexAction = createIndexAction;
        this.updateHelper = new UpdateHelper(scriptService, logger);
        this.indicesService = indicesService;
        this.autoCreateIndex = autoCreateIndex;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
    }

    @Override
    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, UpdateRequest request) {
        super.resolveRequest(metaData, indexMetaData, request);
        resolveAndValidateRouting(metaData, indexMetaData.getIndex().getName(), request);
        ShardId shardId = clusterService.operationRouting().shardId(clusterService.state(),
                indexMetaData.getIndex().getName(), request.id(), request.routing());
        request.setShardId(shardId);
    }

    public static void resolveAndValidateRouting(MetaData metaData, String concreteIndex, UpdateRequest request) {
        request.routing((metaData.resolveIndexRouting(request.parent(), request.routing(), request.index())));
        // Fail fast on the node that received the request, rather than failing when translating on the index or delete request.
        if (request.routing() == null && metaData.routingRequired(concreteIndex, request.type())) {
            throw new RoutingMissingException(concreteIndex, request.type(), request.id());
        }
    }

    @Override
    protected void doExecute(Task task, UpdateRequest request, ActionListener<UpdateResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.index(request.index());
            createIndexRequest.cause("auto(update api)");
            createIndexRequest.masterNodeTimeout(request.timeout());
            createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(task, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(task, request, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(task, request, listener);
        }
    }

    @Override
    protected UpdateResponse newResponseInstance() {
        return new UpdateResponse();
    }

    private void innerExecute(Task task, final UpdateRequest request, final ActionListener<UpdateResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected WriteResult<UpdateReplicaRequest, UpdateResponse> onPrimaryShard(UpdateRequest request, IndexShard indexShard) throws Exception {
        ShardId shardId = request.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexMetaData indexMetaData = indexService.getMetaData();
        return executeUpdateRequestOnPrimary(request, indexShard, indexMetaData, updateHelper, mappingUpdatedAction, allowIdGeneration);
    }

    public static WriteResult<UpdateReplicaRequest, UpdateResponse> executeUpdateRequestOnPrimary(UpdateRequest request,
                                                                                                  IndexShard indexShard,
                                                                                                  IndexMetaData indexMetaData,
                                                                                                  UpdateHelper updateHelper,
                                                                                                  MappingUpdatedAction mappingUpdatedAction,
                                                                                                  boolean allowIdGeneration)
            throws Exception {
        int maxAttempts = request.retryOnConflict();
        for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
            try {
                return shardUpdateOperation(indexMetaData, indexShard, request, updateHelper, mappingUpdatedAction, allowIdGeneration);
            } catch (Exception e) {
                final Throwable cause = ExceptionsHelper.unwrapCause(e);
                if (attemptCount == maxAttempts  // bubble up exception when we run out of attempts
                        || (cause instanceof VersionConflictEngineException) == false) { // or when exception is not a version conflict
                    throw e;
                }
            }
        }
        throw new IllegalStateException("version conflict exception should bubble up on last attempt");

    }

    private static WriteResult<UpdateReplicaRequest, UpdateResponse> shardUpdateOperation(IndexMetaData indexMetaData,
                                                                                          IndexShard indexShard,
                                                                                          UpdateRequest request,
                                                                                          UpdateHelper updateHelper,
                                                                                          MappingUpdatedAction mappingUpdatedAction,
                                                                                          boolean allowIdGeneration)
            throws Exception {
        final UpdateHelper.Result result = updateHelper.prepare(request, indexShard);
        switch (result.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = result.action();
                MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
                indexRequest.process(mappingMd, allowIdGeneration, indexMetaData.getIndex().getName());
                WriteResult<IndexRequest, IndexResponse> indexResponseWriteResult = TransportIndexAction.executeIndexRequestOnPrimary(indexRequest, indexShard, mappingUpdatedAction);
                IndexResponse response = indexResponseWriteResult.getResponse();
                UpdateResponse update = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                // we fetch it from the index request so we don't generate the bytes twice, its already done in the index request
                final BytesReference indexSourceBytes = indexRequest.source();
                if (result.getResponseResult() == DocWriteResponse.Result.CREATED) {
                    if ((request.fetchSource() != null && request.fetchSource().fetchSource()) ||
                            (request.fields() != null && request.fields().length > 0)) {
                        Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceBytes, true);
                        update.setGetResult(updateHelper.extractGetResult(request, indexMetaData.getIndex().getName(), response.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceBytes));
                    } else {
                        update.setGetResult(null);
                    }
                } else if (result.getResponseResult() == DocWriteResponse.Result.UPDATED) {
                    update.setGetResult(updateHelper.extractGetResult(request, indexMetaData.getIndex().getName(), response.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), indexSourceBytes));
                }
                update.setForcedRefresh(response.forcedRefresh());
                UpdateReplicaRequest updateReplicaRequest = new UpdateReplicaRequest(indexRequest);
                updateReplicaRequest.setParentTask(request.getParentTask());
                updateReplicaRequest.setShardId(request.shardId());
                updateReplicaRequest.setRefreshPolicy(request.getRefreshPolicy());
                return new WriteResult<>(updateReplicaRequest, update, indexResponseWriteResult.getLocation());
            case DELETED:
                DeleteRequest deleteRequest = result.action();
                WriteResult<DeleteRequest, DeleteResponse> deleteResponseWriteResult = TransportDeleteAction.executeDeleteRequestOnPrimary(deleteRequest, indexShard);
                DeleteResponse deleteResponse = deleteResponseWriteResult.getResponse();
                UpdateResponse deleteUpdate = new UpdateResponse(deleteResponse.getShardInfo(), deleteResponse.getShardId(), deleteResponse.getType(), deleteResponse.getId(), deleteResponse.getVersion(), deleteResponse.getResult());
                deleteUpdate.setGetResult(updateHelper.extractGetResult(request, indexMetaData.getIndex().getName(), deleteResponse.getVersion(), result.updatedSourceAsMap(), result.updateSourceContentType(), null));
                deleteUpdate.setForcedRefresh(deleteResponse.forcedRefresh());
                UpdateReplicaRequest deleteReplicaRequest = new UpdateReplicaRequest(deleteRequest);
                deleteReplicaRequest.setParentTask(request.getParentTask());
                deleteReplicaRequest.setShardId(request.shardId());
                deleteReplicaRequest.setRefreshPolicy(request.getRefreshPolicy());
                return new WriteResult<>(deleteReplicaRequest, deleteUpdate, deleteResponseWriteResult.getLocation());
            case NOOP:
                UpdateResponse noopUpdate = result.action();
                indexShard.noopUpdate(request.type());
                return new WriteResult<>(null, noopUpdate, null);
            default:
                throw new IllegalStateException("Illegal result " + result.getResponseResult());
        }
    }

    @Override
    protected Translog.Location onReplicaShard(UpdateReplicaRequest request, IndexShard indexShard) {
        assert request.getRequest() != null;
        final Translog.Location location;
        switch (request.getRequest().opType()) {
            case INDEX:
            case CREATE:
                location = TransportIndexAction.executeIndexRequestOnReplica(((IndexRequest) request.getRequest()), indexShard).getTranslogLocation();
                break;
            case DELETE:
                location = TransportDeleteAction.executeDeleteRequestOnReplica(((DeleteRequest) request.getRequest()), indexShard).getTranslogLocation();
                break;
            default:
                throw new IllegalStateException("unexpected opType [" + request.getRequest().opType().getLowercase() + "]");

        }
        return location;
    }
}
