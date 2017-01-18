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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.delete.TransportDeleteAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.action.support.replication.ReplicationOperation.ignoreReplicaException;
import static org.elasticsearch.action.support.replication.ReplicationOperation.isConflictException;

/** Performs shard-level bulk (index, delete or update) operations */
public class TransportShardBulkAction extends TransportWriteAction<BulkShardRequest, BulkShardResponse> {

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
                indexNameExpressionResolver, BulkShardRequest::new, ThreadPool.Names.BULK);
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
    protected WriteResult<BulkShardResponse> onPrimaryShard(BulkShardRequest request, IndexShard primary) throws Exception {
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
        return new WriteResult<>(response, location);
    }

    /** Executes bulk item requests and handles request execution exceptions */
    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard indexShard,
                                                     BulkShardRequest request,
                                                     long[] preVersions, VersionType[] preVersionTypes,
                                                     Translog.Location location, int requestIndex) {
        preVersions[requestIndex] = request.items()[requestIndex].request().version();
        preVersionTypes[requestIndex] = request.items()[requestIndex].request().versionType();
        DocWriteRequest.OpType opType = request.items()[requestIndex].request().opType();
        try {
            WriteResult<? extends DocWriteResponse> writeResult = innerExecuteBulkItemRequest(metaData, indexShard,
                request, requestIndex);
            if (writeResult.getLocation() != null) {
                location = locationToSync(location, writeResult.getLocation());
            } else {
               assert writeResult.getResponse().getResult() == DocWriteResponse.Result.NOOP
                       : "only noop operation can have null next operation";
            }
            // update the bulk item request because update request execution can mutate the bulk item request
            BulkItemRequest item = request.items()[requestIndex];
            // add the response
            setResponse(item, new BulkItemResponse(item.id(), opType, writeResult.getResponse()));
        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < requestIndex; j++) {
                    DocWriteRequest docWriteRequest = request.items()[j].request();
                    docWriteRequest.version(preVersions[j]);
                    docWriteRequest.versionType(preVersionTypes[j]);
                }
                throw (ElasticsearchException) e;
            }
            BulkItemRequest item = request.items()[requestIndex];
            DocWriteRequest docWriteRequest = item.request();
            if (isConflictException(e)) {
                logger.trace((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                                request.shardId(), docWriteRequest.opType().getLowercase(), request), e);
            } else {
                logger.debug((Supplier<?>) () -> new ParameterizedMessage("{} failed to execute bulk item ({}) {}",
                                request.shardId(), docWriteRequest.opType().getLowercase(), request), e);
            }
            // if its a conflict failure, and we already executed the request on a primary (and we execute it
            // again, due to primary relocation and only processing up to N bulk items when the shard gets closed)
            // then just use the response we got from the successful execution
            if (item.getPrimaryResponse() != null && isConflictException(e)) {
                setResponse(item, item.getPrimaryResponse());
            } else {
                setResponse(item, new BulkItemResponse(item.id(), docWriteRequest.opType(),
                                new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), e)));
            }
        }
        assert request.items()[requestIndex].getPrimaryResponse() != null;
        assert preVersionTypes[requestIndex] != null;
        return location;
    }

    private WriteResult<? extends DocWriteResponse> innerExecuteBulkItemRequest(IndexMetaData metaData, IndexShard indexShard,
                                                                                BulkShardRequest request, int requestIndex) throws Exception {
        DocWriteRequest itemRequest = request.items()[requestIndex].request();
        switch (itemRequest.opType()) {
            case CREATE:
            case INDEX:
                return TransportIndexAction.executeIndexRequestOnPrimary(((IndexRequest) itemRequest), indexShard, mappingUpdatedAction);
            case UPDATE:
                int maxAttempts = ((UpdateRequest) itemRequest).retryOnConflict();
                for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
                    try {
                        return shardUpdateOperation(metaData, indexShard, request, requestIndex, ((UpdateRequest) itemRequest));
                    } catch (Exception e) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(e);
                        if (attemptCount == maxAttempts  // bubble up exception when we run out of attempts
                                || (cause instanceof VersionConflictEngineException) == false) { // or when exception is not a version conflict
                            throw e;
                         }
                     }
                }
                throw new IllegalStateException("version conflict exception should bubble up on last attempt");
            case DELETE:
                return TransportDeleteAction.executeDeleteRequestOnPrimary(((DeleteRequest) itemRequest), indexShard);
            default: throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
        }
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

    /**
     * Executes update request, doing a get and translating update to a index or delete operation
     * NOTE: all operations except NOOP, reassigns the bulk item request
     */
    private WriteResult<? extends DocWriteResponse> shardUpdateOperation(IndexMetaData metaData, IndexShard indexShard,
                                                                         BulkShardRequest request,
                                                                         int requestIndex, UpdateRequest updateRequest)
        throws Exception {
        // Todo: capture read version conflicts, missing documents and malformed script errors in the write result due to get request
        UpdateHelper.Result translate = updateHelper.prepare(updateRequest, indexShard, threadPool::estimatedTimeInMillis);
        switch (translate.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = translate.action();
                MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                indexRequest.process(mappingMd, allowIdGeneration, request.index());
                WriteResult<IndexResponse> writeResult = TransportIndexAction.executeIndexRequestOnPrimary(indexRequest, indexShard, mappingUpdatedAction);
                BytesReference indexSourceAsBytes = indexRequest.source();
                IndexResponse indexResponse = writeResult.getResponse();
                UpdateResponse update = new UpdateResponse(indexResponse.getShardInfo(), indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(), indexResponse.getVersion(), indexResponse.getResult());
                if ((updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) ||
                        (updateRequest.fields() != null && updateRequest.fields().length > 0)) {
                    Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(indexSourceAsBytes, true);
                    update.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                }
                // Replace the update request to the translated index request to execute on the replica.
                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), indexRequest);
                return new WriteResult<>(update, writeResult.getLocation());
            case DELETED:
                DeleteRequest deleteRequest = translate.action();
                WriteResult<DeleteResponse> deleteResult = TransportDeleteAction.executeDeleteRequestOnPrimary(deleteRequest, indexShard);
                DeleteResponse response = deleteResult.getResponse();
                UpdateResponse deleteUpdateResponse = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                deleteUpdateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), response.getVersion(), translate.updatedSourceAsMap(), translate.updateSourceContentType(), null));
                // Replace the update request to the translated delete request to execute on the replica.
                request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), deleteRequest);
                return new WriteResult<>(deleteUpdateResponse, deleteResult.getLocation());
            case NOOP:
                BulkItemRequest item = request.items()[requestIndex];
                indexShard.noopUpdate(updateRequest.type());
                item.setIgnoreOnReplica(); // no need to go to the replica
                return new WriteResult<>(translate.action(), null);
            default: throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());        }
    }

    @Override
    protected Location onReplicaShard(BulkShardRequest request, IndexShard indexShard) {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null || item.isIgnoreOnReplica()) {
                continue;
            }
            DocWriteRequest docWriteRequest = item.request();
            final Engine.Operation operation;
            try {
                switch (docWriteRequest.opType()) {
                    case CREATE:
                    case INDEX:
                        operation = TransportIndexAction.executeIndexRequestOnReplica(((IndexRequest) docWriteRequest), indexShard);
                        break;
                    case DELETE:
                        operation = TransportDeleteAction.executeDeleteRequestOnReplica(((DeleteRequest) docWriteRequest), indexShard);
                        break;
                    default: throw new IllegalStateException("Unexpected request operation type on replica: "
                            + docWriteRequest.opType().getLowercase());
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
