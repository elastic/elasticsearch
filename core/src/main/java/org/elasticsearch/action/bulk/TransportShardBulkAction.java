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
import org.elasticsearch.index.engine.Engine;
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
import static org.elasticsearch.action.index.TransportIndexAction.executeIndexRequestOnReplica;
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
    protected WritePrimaryResult onPrimaryShard(BulkShardRequest request, IndexShard primary) throws Exception {
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
        return new WritePrimaryResult(request, response, location, null, primary);
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
            // execute item request
            DocWriteRequest itemRequest = request.items()[requestIndex].request();
            final Engine.Operation operation;
            final DocWriteResponse response;
            switch (itemRequest.opType()) {
                case CREATE:
                case INDEX:
                    final IndexRequest indexRequest = (IndexRequest) itemRequest;
                    operation = executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
                    response = operation.hasFailure() ? null
                            : new IndexResponse(primary.shardId(), indexRequest.type(), indexRequest.id(),
                                operation.version(), ((Engine.Index) operation).isCreated());
                    break;
                case UPDATE:
                    Engine.Operation updateOperation = null;
                    UpdateResponse updateResponse = null;
                    UpdateRequest updateRequest = (UpdateRequest) itemRequest;
                    int maxAttempts = updateRequest.retryOnConflict();
                    for (int attemptCount = 0; attemptCount <= maxAttempts; attemptCount++) {
                        final UpdateHelper.Result translate;
                        try {
                            // translate and execute operation
                            translate = updateHelper.prepare(updateRequest, primary, threadPool::estimatedTimeInMillis);
                        } catch (Exception failure) {
                            // we may fail translating a update to index or delete operation
                            updateOperation = new Engine.Failure(updateRequest.type(), updateRequest.id(), updateRequest.version(),
                                    updateRequest.versionType(), Engine.Operation.Origin.PRIMARY, System.nanoTime(), failure);
                            break; // out of retry loop
                        }
                        updateOperation = shardUpdateOperation(metaData, primary, request, updateRequest, translate);
                        if (updateOperation == null) {
                            // this is a noop operation
                            updateResponse = translate.action();
                        } else {
                            if (updateOperation.hasFailure() == false) {
                                // enrich update response and
                                // set translated update (index/delete) request for replica execution in bulk items
                                switch (updateOperation.operationType()) {
                                    case INDEX:
                                        IndexRequest updateIndexRequest = translate.action();
                                        final IndexResponse indexResponse = new IndexResponse(primary.shardId(),
                                                updateIndexRequest.type(), updateIndexRequest.id(),
                                                updateOperation.version(), ((Engine.Index) updateOperation).isCreated());
                                        BytesReference indexSourceAsBytes = updateIndexRequest.source();
                                        updateResponse = new UpdateResponse(indexResponse.getShardInfo(),
                                                indexResponse.getShardId(), indexResponse.getType(), indexResponse.getId(),
                                                indexResponse.getVersion(), indexResponse.getResult());
                                        if ((updateRequest.fetchSource() != null && updateRequest.fetchSource().fetchSource()) ||
                                                (updateRequest.fields() != null && updateRequest.fields().length > 0)) {
                                            Tuple<XContentType, Map<String, Object>> sourceAndContent =
                                                    XContentHelper.convertToMap(indexSourceAsBytes, true);
                                            updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest, request.index(),
                                                    indexResponse.getVersion(), sourceAndContent.v2(), sourceAndContent.v1(), indexSourceAsBytes));
                                        }
                                        // replace the update request to the translated index request to execute on the replica.
                                        request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), updateIndexRequest);
                                        break;
                                    case DELETE:
                                        DeleteRequest updateDeleteRequest = translate.action();
                                        DeleteResponse deleteResponse = new DeleteResponse(primary.shardId(),
                                                updateDeleteRequest.type(), updateDeleteRequest.id(),
                                                updateOperation.version(), ((Engine.Delete) updateOperation).found());
                                        updateResponse = new UpdateResponse(deleteResponse.getShardInfo(),
                                                deleteResponse.getShardId(), deleteResponse.getType(), deleteResponse.getId(),
                                                deleteResponse.getVersion(), deleteResponse.getResult());
                                        updateResponse.setGetResult(updateHelper.extractGetResult(updateRequest,
                                                request.index(), deleteResponse.getVersion(), translate.updatedSourceAsMap(),
                                                translate.updateSourceContentType(), null));
                                        // replace the update request to the translated delete request to execute on the replica.
                                        request.items()[requestIndex] = new BulkItemRequest(request.items()[requestIndex].id(), updateDeleteRequest);
                                        break;
                                    case FAILURE:
                                        break;
                                }
                            } else {
                                // version conflict exception, retry
                                if (updateOperation.getFailure() instanceof VersionConflictEngineException) {
                                    continue;
                                }
                            }
                        }
                        break; // out of retry loop
                    }
                    operation = updateOperation;
                    response = updateResponse;
                    break;
                case DELETE:
                    final DeleteRequest deleteRequest = (DeleteRequest) itemRequest;
                    operation = executeDeleteRequestOnPrimary(deleteRequest, primary);
                    response = operation.hasFailure() ? null :
                            new DeleteResponse(request.shardId(), deleteRequest.type(), deleteRequest.id(),
                                operation.version(), ((Engine.Delete) operation).found());
                    break;
                default: throw new IllegalStateException("unexpected opType [" + itemRequest.opType() + "] found");
            }
            // set item response and handle failures

            // update the bulk item request because update request execution can mutate the bulk item request
            BulkItemRequest item = request.items()[requestIndex];

            if (operation == null // in case of a noop update operation
                    || operation.hasFailure() == false) {
                if (operation == null) {
                    assert response != null;
                    assert response.getResult() == DocWriteResponse.Result.NOOP
                            : "only noop operation can have null operation";
                } else {
                    location = locationToSync(location, operation.getTranslogLocation());
                }
                // add the response
                item.setPrimaryResponse(new BulkItemResponse(item.id(), opType, response));
            } else {
                DocWriteRequest docWriteRequest = item.request();
                Exception failure = operation.getFailure();
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
                if (item.getPrimaryResponse() == null || isConflictException(failure) == false) {
                    item.setPrimaryResponse(new BulkItemResponse(item.id(), docWriteRequest.opType(),
                            new BulkItemResponse.Failure(request.index(), docWriteRequest.type(), docWriteRequest.id(), failure)));
                }
            }
            assert item.getPrimaryResponse() != null;
            assert preVersionTypes[requestIndex] != null;
            if (item.getPrimaryResponse().isFailed()
                    || item.getPrimaryResponse().getResponse().getResult() == DocWriteResponse.Result.NOOP) {
                item.setIgnoreOnReplica();
            } else {
                // set the ShardInfo to 0 so we can safely send it to the replicas. We won't use it in the real response though.
                item.getPrimaryResponse().getResponse().setShardInfo(new ShardInfo());
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
        return location;
    }

    /** Executes update request, delegating to a index or delete operation after translation */
    private Engine.Operation shardUpdateOperation(IndexMetaData metaData, IndexShard primary, BulkShardRequest request,
                                                  UpdateRequest updateRequest, UpdateHelper.Result translate)
        throws Exception {
        switch (translate.getResponseResult()) {
            case CREATED:
            case UPDATED:
                IndexRequest indexRequest = translate.action();
                MappingMetaData mappingMd = metaData.mappingOrDefault(indexRequest.type());
                indexRequest.process(mappingMd, allowIdGeneration, request.index());
                return executeIndexRequestOnPrimary(indexRequest, primary, mappingUpdatedAction);
            case DELETED:
                return executeDeleteRequestOnPrimary(translate.action(), primary);
            case NOOP:
                primary.noopUpdate(updateRequest.type());
                return null;
            default: throw new IllegalStateException("Illegal update operation " + translate.getResponseResult());
        }
    }

    @Override
    protected WriteReplicaResult onReplicaShard(BulkShardRequest request, IndexShard replica) throws Exception {
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
                        operation = executeIndexRequestOnReplica(((IndexRequest) docWriteRequest), replica);
                        break;
                    case DELETE:
                        operation = executeDeleteRequestOnReplica(((DeleteRequest) docWriteRequest), replica);
                        break;
                    default: throw new IllegalStateException("Unexpected request operation type on replica: "
                            + docWriteRequest.opType().getLowercase());
                }
                if (operation.hasFailure()) {
                    // check if any transient write operation failures should be bubbled up
                    Exception failure = operation.getFailure();
                    if (!ignoreReplicaException(failure)) {
                        throw failure;
                    }
                } else {
                    location = locationToSync(location, operation.getTranslogLocation());
                }
            } catch (Exception e) {
                // if its not an ignore replica failure, we need to make sure to bubble up the failure
                // so we will fail the shard
                if (!ignoreReplicaException(e)) {
                    throw e;
                }
            }
        }
        return new WriteReplicaResult(request, location, null, replica);
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
