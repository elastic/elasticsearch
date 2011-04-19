/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Strings;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the index operation.
 *
 * @author kimchy (shay.banon)
 */
public class TransportShardBulkAction extends TransportShardReplicationOperationAction<BulkShardRequest, BulkShardResponse> {

    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject public TransportShardBulkAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                            IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                            MappingUpdatedAction mappingUpdatedAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.mappingUpdatedAction = mappingUpdatedAction;
    }

    @Override protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override protected boolean checkWriteConsistency() {
        return true;
    }

    @Override protected TransportRequestOptions transportOptions() {
        // low type since we don't want the large bulk requests to cause high latency on typical requests
        return TransportRequestOptions.options().withCompress(true).withLowType();
    }

    @Override protected BulkShardRequest newRequestInstance() {
        return new BulkShardRequest();
    }

    @Override protected BulkShardResponse newResponseInstance() {
        return new BulkShardResponse();
    }

    @Override protected String transportAction() {
        return "indices/index/shard/bulk";
    }

    @Override protected void checkBlock(BulkShardRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override protected ShardIterator shards(ClusterState clusterState, BulkShardRequest request) {
        return clusterState.routingTable().index(request.index()).shard(request.shardId()).shardsIt();
    }

    @Override protected PrimaryResponse<BulkShardResponse> shardOperationOnPrimary(ClusterState clusterState, ShardOperationRequest shardRequest) {
        final BulkShardRequest request = shardRequest.request;
        IndexShard indexShard = indexShard(shardRequest);

        Engine.IndexingOperation[] ops = null;

        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {

                    // validate, if routing is required, that we got routing
                    MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mapping(indexRequest.type());
                    if (mappingMd != null && mappingMd.routing().required()) {
                        if (indexRequest.routing() == null) {
                            throw new RoutingMissingException(indexRequest.index(), indexRequest.type(), indexRequest.id());
                        }
                    }

                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent());
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
                        updateMappingOnMaster(indexRequest);
                    }

                    // if we are going to percolate, then we need to keep this op for the postPrimary operation
                    if (Strings.hasLength(indexRequest.percolate())) {
                        if (ops == null) {
                            ops = new Engine.IndexingOperation[request.items().length];
                        }
                        ops[i] = op;
                    }

                    // add the response
                    responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().toString().toLowerCase(),
                            new IndexResponse(indexRequest.index(), indexRequest.type(), indexRequest.id(), version));
                } catch (Exception e) {
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + shardRequest.request.index() + "][" + shardRequest.shardId + "]" + ": Failed to execute bulk item (index) [" + indexRequest + "]", e);
                    }
                    responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().toString().toLowerCase(),
                            new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), ExceptionsHelper.detailedMessage(e)));
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
                    if (logger.isDebugEnabled()) {
                        logger.debug("[" + shardRequest.request.index() + "][" + shardRequest.shardId + "]" + ": Failed to execute bulk item (delete) [" + deleteRequest + "]", e);
                    }
                    responses[i] = new BulkItemResponse(item.id(), "delete",
                            new BulkItemResponse.Failure(deleteRequest.index(), deleteRequest.type(), deleteRequest.id(), ExceptionsHelper.detailedMessage(e)));
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
        BulkShardResponse response = new BulkShardResponse(new ShardId(request.index(), request.shardId()), responses);
        return new PrimaryResponse<BulkShardResponse>(response, ops);
    }

    @Override protected void postPrimaryOperation(BulkShardRequest request, PrimaryResponse<BulkShardResponse> response) {
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

    @Override protected void shardOperationOnReplica(ShardOperationRequest shardRequest) {
        IndexShard indexShard = indexShard(shardRequest);
        final BulkShardRequest request = shardRequest.request;
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.request() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.request();
                try {
                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.source()).type(indexRequest.type()).id(indexRequest.id())
                            .routing(indexRequest.routing()).parent(indexRequest.parent());
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

    private void updateMappingOnMaster(final IndexRequest request) {
        try {
            MapperService mapperService = indicesService.indexServiceSafe(request.index()).mapperService();
            final DocumentMapper documentMapper = mapperService.documentMapper(request.type());
            if (documentMapper == null) { // should not happen
                return;
            }
            documentMapper.refreshSource();

            mappingUpdatedAction.execute(new MappingUpdatedAction.MappingUpdatedRequest(request.index(), request.type(), documentMapper.mappingSource()), new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                }

                @Override public void onFailure(Throwable e) {
                    try {
                        logger.warn("Failed to update master on updated mapping for index [" + request.index() + "], type [" + request.type() + "] and source [" + documentMapper.mappingSource().string() + "]", e);
                    } catch (IOException e1) {
                        // ignore
                    }
                }
            });
        } catch (Exception e) {
            logger.warn("Failed to update master on updated mapping for index [" + request.index() + "], type [" + request.type() + "]", e);
        }
    }
}
