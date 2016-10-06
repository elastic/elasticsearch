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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocumentRequest;
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
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.action.update.UpdateHelper;
import org.elasticsearch.action.update.UpdateReplicaRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.ClusterState;
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
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.action.support.replication.ReplicationOperation.ignoreReplicaException;
import static org.elasticsearch.action.support.replication.ReplicationOperation.isConflictException;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    public static final String ACTION_NAME = BulkAction.NAME + "[s]";

    private final boolean allowIdGeneration;
    private final MappingUpdatedAction mappingUpdatedAction;
    private final UpdateHelper updateHelper;
    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction, ActionFilters actionFilters,
                                    IndexNameExpressionResolver indexNameExpressionResolver, ScriptService scriptService,
                                    AutoCreateIndex autoCreateIndex, TransportCreateIndexAction createIndexAction) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, BulkShardRequest::new, BulkShardRequest::new, ThreadPool.Names.BULK);
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.updateHelper = new UpdateHelper(scriptService, logger);
        this.autoCreateIndex = autoCreateIndex;
        this.createIndexAction = createIndexAction;
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
    protected void doExecute(Task task, BulkShardRequest request, ActionListener<BulkShardResponse> listener) {
        ClusterState state = clusterService.state();
        if (autoCreateIndex.shouldAutoCreate(request.index(), state)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.index(request.index());
            createIndexRequest.cause("auto(bulk api)");
            createIndexRequest.masterNodeTimeout(request.timeout());
            createIndexAction.execute(task, createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(task, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        innerExecute(task, request, listener);
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(task, request, listener);
        }
    }

    private void innerExecute(Task task, final BulkShardRequest request, final ActionListener<BulkShardResponse> listener) {
        super.doExecute(task, request, listener);
    }
    @Override
    protected WriteResult<BulkShardRequest, BulkShardResponse> onPrimaryShard(BulkShardRequest request, IndexShard indexShard) throws Exception {
        ShardId shardId = request.shardId();
        final IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        final IndexMetaData metaData = indexService.getIndexSettings().getIndexMetaData();

        long[] preVersions = new long[request.items().length];
        VersionType[] preVersionTypes = new VersionType[request.items().length];
        Translog.Location location = null;
        for (int requestIndex = 0; requestIndex < request.items().length; requestIndex++) {
            location = executeBulkItemRequest(metaData, indexShard, request, preVersions, preVersionTypes, location, requestIndex);
        }

        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        BulkItemRequest[] items = request.items();
        for (int i = 0; i < items.length; i++) {
            responses[i] = items[i].getPrimaryResponse();
        }
        BulkShardResponse response = new BulkShardResponse(request.shardId(), responses);
        return new WriteResult<>(request, response, location);
    }

    /** Executes bulk item requests and handles request execution exceptions */
    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard indexShard,
                                                     BulkShardRequest request,
                                                     long[] preVersions, VersionType[] preVersionTypes,
                                                     Translog.Location location, int requestIndex) {
        DocumentRequest<?> itemRequest = request.items()[requestIndex].request();
        preVersions[requestIndex] = itemRequest.version();
        preVersionTypes[requestIndex] = itemRequest.versionType();
        DocumentRequest.OpType opType = itemRequest.opType();
        try {
            final WriteResult<? extends ReplicatedWriteRequest, ? extends DocWriteResponse> writeResult;
            switch (itemRequest.opType()) {
                case CREATE:
                case INDEX:
                    writeResult = TransportIndexAction.executeIndexRequestOnPrimary(((IndexRequest) itemRequest), indexShard,
                            mappingUpdatedAction);
                    break;
                case UPDATE:
                    writeResult = TransportUpdateAction.executeUpdateRequestOnPrimary(((UpdateRequest) itemRequest), indexShard,
                            metaData, updateHelper, mappingUpdatedAction, allowIdGeneration);
                    break;
                case DELETE:
                    writeResult = TransportDeleteAction.executeDeleteRequestOnPrimary(((DeleteRequest) itemRequest), indexShard);
                    break;
                default:
                    throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
            }
            if (writeResult.getLocation() != null) {
                location = locationToSync(location, writeResult.getLocation());
            } else {
                assert writeResult.getResponse().getResult() == DocWriteResponse.Result.NOOP
                        : "only noop operation can have null next operation";
            }
            // update the bulk item request with replica request (update request are changed to index or delete requests for replication)
            request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(),
                    (DocumentRequest<?>) writeResult.getReplicaRequest());
            // add the response
            setResponse(request.items()[requestIndex], new BulkItemResponse(request.items()[requestIndex].id(), opType, writeResult.getResponse()));
        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < requestIndex; j++) {
                    DocumentRequest<?> documentRequest = request.items()[j].request();
                    documentRequest.version(preVersions[j]);
                    documentRequest.versionType(preVersionTypes[j]);
                }
                throw (ElasticsearchException) e;
            }
            BulkItemRequest item = request.items()[requestIndex];
            DocumentRequest<?> documentRequest = item.request();
            if (isConflictException(e)) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                        request.shardId(), documentRequest.opType().getLowercase(), request), e);
            } else {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                        request.shardId(), documentRequest.opType().getLowercase(), request), e);
            }
            // if its a conflict failure, and we already executed the request on a primary (and we execute it
            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
            // then just use the response we got from the successful execution
            if (item.getPrimaryResponse() != null && isConflictException(e)) {
                setResponse(item, item.getPrimaryResponse());
            } else {
                setResponse(item, new BulkItemResponse(item.id(), documentRequest.opType(),
                    new BulkItemResponse.Failure(request.index(), documentRequest.type(), documentRequest.id(), e)));
            }
        }
        assert request.items()[requestIndex].getPrimaryResponse() != null;
        assert preVersionTypes[requestIndex] != null;
        return location;
    }

    private void setResponse(BulkItemRequest request, BulkItemResponse response) {
        request.setPrimaryResponse(response);
        if (response.isFailed()) {
            request.setIgnoreOnReplica();
        } else {
            // Set the ShardInfo to 0 so we can safely send it to the replicas. We won't use it in the real response though.
            response.getResponse().setShardInfo(new ShardInfo());
        }
    }

    @Override
    protected Location onReplicaShard(BulkShardRequest request, IndexShard indexShard) {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null || item.isIgnoreOnReplica()) {
                continue;
            }
            DocumentRequest documentRequest = (item.request() instanceof UpdateReplicaRequest)
                    ? ((UpdateReplicaRequest) item.request()).getRequest() : item.request();
            final Engine.Operation operation;
            try {
                switch (documentRequest.opType()) {
                    case CREATE:
                    case INDEX:
                        operation = TransportIndexAction.executeIndexRequestOnReplica(((IndexRequest) documentRequest), indexShard);
                        break;
                    case DELETE:
                        operation = TransportDeleteAction.executeDeleteRequestOnReplica(((DeleteRequest) documentRequest), indexShard);
                        break;
                    default: throw new IllegalStateException("Unexpected request operation type on replica: "
                            + documentRequest.opType().getLowercase());
                }
                location = locationToSync(location, operation.getTranslogLocation());
            } catch (Exception e) {
                // if its not an ignore replica failure, we need to make sure to bubble up the failure
                // so we will fail the shard
                if (!ignoreReplicaException(e)) {
                    throw e;
                }
            }
        }
        return location;
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
}
