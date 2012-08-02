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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
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

import java.io.IOException;
import java.util.Locale;
import java.util.Set;

/**
 * Performs the index operation.
 */
public class TransportShardBulkAction extends TransportShardReplicationOperationAction<BulkShardRequest, BulkShardRequest, BulkShardResponse> {

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                    IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                    MappingUpdatedAction mappingUpdatedAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.mappingUpdatedAction = mappingUpdatedAction;
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
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {

                    // validate, if routing is required, that we got routing
                    MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mappingOrDefault(indexRequest.type());
                    if (mappingMd != null && mappingMd.routing().required()) {
                        if (indexRequest.routing() == null) {
                            throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
                        }
                    }

                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
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
                    // update the version on request so it will happen on the replicas
                    indexRequest.version(version);

                    // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
                    if (op.parsedDoc().mappersAdded()) {
                        if (mappingsToUpdate == null) {
                            mappingsToUpdate = Sets.newHashSet();
                        }
                        mappingsToUpdate.add(Tuple.tuple(indexRequest.index(), indexRequest.type()));
                    }

                    // if we are going to percolate, then we need to keep this op for the postPrimary operation
                    if (Strings.hasLength(indexRequest.percolate())) {
                        if (ops == null) {
                            ops = new Engine.IndexingOperation[request.items().length];
                        }
                        ops[i] = op;
                    }

                    // add the response
                    responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().toString().toLowerCase(Locale.ENGLISH),
                            new IndexResponse(indexRequest.index(), indexRequest.type(), indexRequest.id(), version));
                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
                    }
                    responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().toString().toLowerCase(Locale.ENGLISH),
                            new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items()[i] = null;
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).versionType(deleteRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
                    indexShard.delete(delete);
                    // update the request with teh version so it will go to the replicas
                    deleteRequest.version(delete.version());

                    // add the response
                    responses[i] = new BulkItemResponse(item.id(), "delete",
                            new DeleteResponse(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), delete.version(), delete.notFound()));
                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.index(), shardRequest.shardId, deleteRequest);
                    }
                    responses[i] = new BulkItemResponse(item.id(), "delete",
                            new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.items()[i] = null;
                }
            }
        }

        if (mappingsToUpdate != null) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2());
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh(false));
            } catch (Exception e) {
                // ignore
            }
        }
        BulkShardResponse response = new BulkShardResponse(new ShardId(request.index(), request.shardId()), responses);
        return new PrimaryResponse<BulkShardResponse, BulkShardRequest>(shardRequest.request, response, ops);
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
            BulkItemResponse itemResponse = response.response().responses()[i];
            if (itemResponse.failed()) {
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
                    ((IndexResponse) itemResponse.response()).matches(percolate.matches());
                } catch (Exception e) {
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
                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent()).timestamp(indexRequest.timestamp()).ttl(indexRequest.ttl());

                    if (indexRequest.opType() == IndexRequest.OpType.INDEX) {
                        Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.index(index);
                    } else {
                        Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.create(create);
                    }
                } catch (Exception e) {
                    // ignore, we are on backup
                }
            } else if (item.request() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.request();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.type(), deleteRequest.id(), deleteRequest.version()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.delete(delete);
                } catch (Exception e) {
                    // ignore, we are on backup
                }
            }
        }

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh(false));
            } catch (Exception e) {
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
            documentMapper.refreshSource();

            mappingUpdatedAction.execute(new MappingUpdatedAction.MappingUpdatedRequest(index, type, documentMapper.mappingSource()), new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override
                public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                }

                @Override
                public void onFailure(Throwable e) {
                    try {
                        logger.warn("failed to update master on updated mapping for index [{}], type [{}] and source [{}]", e, index, type, documentMapper.mappingSource().string());
                    } catch (IOException e1) {
                        // ignore
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("failed to update master on updated mapping for index [{}], type [{}]", e, index, type);
        }
    }
}
