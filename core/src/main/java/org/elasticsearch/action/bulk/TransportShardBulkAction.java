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
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.index.TransportIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
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
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.util.Map;

import static org.elasticsearch.action.delete.TransportDeleteAction.executeDeleteRequestOnPrimary;
import static org.elasticsearch.action.delete.TransportDeleteAction.executeDeleteRequestOnReplica;
import static org.elasticsearch.action.index.TransportIndexAction.executeIndexRequestOnPrimary;
import static org.elasticsearch.action.support.replication.ReplicationOperation.ignoreReplicaException;
import static org.elasticsearch.action.support.replication.ReplicationOperation.isConflictException;

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
    protected PrimaryOperationResult<BulkShardResponse> onPrimaryShard(BulkShardRequest request, IndexShard primary) throws Exception {
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
        return new PrimaryOperationResult<>(response, location);
    }

    /** Executes bulk item requests and handles request execution exceptions */
    private Translog.Location executeBulkItemRequest(IndexMetaData metaData, IndexShard primary,
                                                     BulkShardRequest request,
                                                     long[] preVersions, VersionType[] preVersionTypes,
                                                     Translog.Location location, int requestIndex) throws Exception {
        preVersions[requestIndex] = request.items()[requestIndex].request().version();
        preVersionTypes[requestIndex] = request.items()[requestIndex].request().versionType();
        DocWriteRequest.OpType opType = request.items()[requestIndex].request().opType();
        try {
            DocWriteRequest itemRequest = request.items()[requestIndex].request();
            final PrimaryOperationResult<? extends DocWriteResponse> primaryOperationResult;
            switch (itemRequest.opType()) {
                case CREATE:
                case INDEX:
                    primaryOperationResult = executeIndexRequestOnPrimary(((IndexRequest) itemRequest), primary, mappingUpdatedAction);
                    break;
                case UPDATE:
                    int maxAttempts = ((UpdateRequest) itemRequest).retryOnConflict();
                    PrimaryOperationResult<? extends DocWriteResponse> shardUpdateOperation = null;
                    for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
                        shardUpdateOperation = shardUpdateOperation(metaData, primary, request, requestIndex, ((UpdateRequest) itemRequest));
                        if (shardUpdateOperation.success()
                                || shardUpdateOperation.getFailure() instanceof VersionConflictEngineException == false) {
                            break;
                        }
                    }
                    if (shardUpdateOperation == null) {
                        throw new IllegalStateException("version conflict exception should bubble up on last attempt");
                    }
                    primaryOperationResult = shardUpdateOperation;
                    break;
                case DELETE:
                    primaryOperationResult = executeDeleteRequestOnPrimary(((DeleteRequest) itemRequest), primary);
                    break;
                default: throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
            }
            if (primaryOperationResult.success()) {
                if (primaryOperationResult.getLocation() != null) {
                    location = locationToSync(location, primaryOperationResult.getLocation());
                } else {
                    assert primaryOperationResult.getResponse().getResult() == DocWriteResponse.Result.NOOP
                            : "only noop operation can have null next operation";
                }
                // update the bulk item request because update request execution can mutate the bulk item request
                BulkItemRequest item = request.items()[requestIndex];
                // add the response
                setResponse(item, new BulkItemResponse(item.id(), opType, primaryOperationResult.getResponse()));
            } else {
                BulkItemRequest item = request.items()[requestIndex];
                DocWriteRequest docWriteRequest = item.request();
                Exception failure = primaryOperationResult.getFailure();
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
                if (item.getPrimaryResponse() != null && isConflictException(failure)) {
                    setResponse(item, item.getPrimaryResponse());
                } else {
                    setResponse(item, new BulkItemResponse(item.id(), docWriteRequest.opType(),
                            new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), failure)));
                }
            }
        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < requestIndex; j++) {
                    DocWriteRequest docWriteRequest = request.items()[j].request();
                    docWriteRequest.version(preVersions[j]);
                    docWriteRequest.versionType(preVersionTypes[j]);
                }
                throw e;
            }
            // TODO: maybe this assert is too strict, we can still get environment failures while executing write operations
            assert false : "unexpected exception: " + e.getMessage() + " class:" + e.getClass().getSimpleName();
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

    /**
     * Executes update request, doing a get and translating update to a index or delete operation
     * NOTE: all operations except NOOP, reassigns the bulk item request
     */
    private PrimaryOperationResult<? extends DocWriteResponse> shardUpdateOperation(IndexMetaData metaData, IndexShard primary,
                                                                         BulkShardRequest request,
                                                                         int requestIndex, UpdateRequest updateRequest)
        throws Exception {
        final UpdateHelper.Result translate;
        try {
            translate = updateHelper.prepare(updateRequest, primary, threadPool::estimatedTimeInMillis);
        } catch (Exception e) {
            return new PrimaryOperationResult<>(e);
        }
        switch (translate.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = translate.action();
                MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                indexRequest.process(mappingMd, allowIdGeneration, request.index());
                PrimaryOperationResult<IndexResponse> writeResult = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                if (writeResult.success()) {
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
                    return new PrimaryOperationResult<>(update, writeResult.getLocation());
                } else {
                    return writeResult;
                }
            case DELETED:
                DeleteRequest deleteRequest = translate.action();
                PrimaryOperationResult<DeleteResponse> deleteResult = executeDeleteRequestOnPrimary(deleteRequest, primary);
                if (deleteResult.success()) {
                    DeleteResponse response = deleteResult.getResponse();
                    UpdateResponse deleteUpdateResponse = new UpdateResponse(response.getShardInfo(), response.getShardId(), response.getType(), response.getId(), response.getVersion(), response.getResult());
                    deleteUpdateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(), response.getVersion(), translate.updatedSourceAsMap(), translate.updateSourceContentType(), null));
                    // Replace the update request to the translated delete request to execute on the replica.
                    request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), deleteRequest);
                    return new PrimaryOperationResult<>(deleteUpdateResponse, deleteResult.getLocation());
                } else {
                    return deleteResult;
                }
            case NOOP:
                BulkItemRequest item = request.items()[requestIndex];
                primary.noopUpdate(updateRequest.type());
                item.setIgnoreOnReplica(); // no need to go to the replica
                return new PrimaryOperationResult<>(translate.action(), null);
            default: throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
        }
    }

    @Override
    protected ReplicaOperationResult onReplicaShard(BulkShardRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item == null || item.isIgnoreOnReplica()) {
                continue;
            }
            DocWriteRequest docWriteRequest = item.request();
            final ReplicaOperationResult replicaResult;
            try {
                switch (docWriteRequest.opType()) {
                    case CREATE:
                    case INDEX:
                        replicaResult = TransportIndexAction.executeIndexRequestOnReplica(((IndexRequest) docWriteRequest), replica);
                        break;
                    case DELETE:
                        replicaResult = executeDeleteRequestOnReplica(((DeleteRequest) docWriteRequest), replica);
                        break;
                    default: throw new IllegalStateException("Unexpected request operation type on replica: "
                            + docWriteRequest.opType().getLowercase());
                }
                if (replicaResult.success()) {
                    location = locationToSync(location, replicaResult.getLocation());
                } else {
                    // check if any transient write operation failures should be bubbled up
                    Exception failure = replicaResult.getFailure();
                    if (!ignoreReplicaException(failure)) {
                        throw failure;
                    }
                }
            } catch (Exception e) {
                // if its not an ignore replica failure, we need to make sure to bubble up the failure
                // so we will fail the shard
                if (!ignoreReplicaException(e)) {
                    throw e;
                }
            }
        }
        return new ReplicaOperationResult(location);
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
