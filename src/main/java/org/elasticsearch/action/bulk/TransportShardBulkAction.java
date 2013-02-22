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
import org.elasticsearch.ElasticSearchGenerationException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.action.update.PartialDocumentUpdateRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.engine.DocumentMissingException;
import org.elasticsearch.index.engine.DocumentSourceMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
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
import java.util.Map;
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
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.getIndex());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, BulkShardRequest request) {
        return clusterState.routingTable().index(request.getIndex()).shard(request.getShardId()).shardsIt();
    }
    
    private void indexRequest(ClusterState clusterState, PrimaryOperationRequest shardRequest,  Engine.IndexingOperation[] ops, long[] versions, BulkItemResponse[] responses, 
    		Set<Tuple<String, String>> mappingsToUpdate, BulkItemRequest item, IndexShard indexShard, BulkShardRequest request, IndexRequest indexRequest, int i) { 
    	try {

            // validate, if routing is required, that we got routing
            MappingMetaData mappingMd = clusterState.metaData().index(request.getIndex()).mappingOrDefault(indexRequest.getType());
            if (mappingMd != null && mappingMd.routing().required()) {
                if (indexRequest.getRouting() == null) {
                    throw new RoutingMissingException(indexRequest.getIndex(), indexRequest.getType(), indexRequest.getId());
                }
            }

            SourceToParse sourceToParse = SourceToParse.source(indexRequest.getSource()).type(indexRequest.getType()).id(indexRequest.getId())
                    .routing(indexRequest.getRouting()).parent(indexRequest.getParent()).timestamp(indexRequest.getTimestamp()).ttl(indexRequest.getTtl());

            long version;
            Engine.IndexingOperation op;
            if (indexRequest.getOpType() == IndexRequest.OpType.INDEX) {
                Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.getVersion()).versionType(indexRequest.getVersionType()).origin(Engine.Operation.Origin.PRIMARY);
                indexShard.index(index);
                version = index.version();
                op = index;
            } else {
                Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.getVersion()).versionType(indexRequest.getVersionType()).origin(Engine.Operation.Origin.PRIMARY);
                indexShard.create(create);
                version = create.version();
                op = create;
            }
            versions[i] = indexRequest.getVersion();
            // update the version on request so it will happen on the replicas
            indexRequest.setVersion(version);

            // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
            if (op.parsedDoc().mappingsModified()) {
                if (mappingsToUpdate == null) {
                    mappingsToUpdate = Sets.newHashSet();
                }
                mappingsToUpdate.add(Tuple.tuple(indexRequest.getIndex(), indexRequest.getType()));
            }

            // if we are going to percolate, then we need to keep this op for the postPrimary operation
            if (Strings.hasLength(indexRequest.getPercolate())) {
                if (ops == null) {
                    ops = new Engine.IndexingOperation[request.getItems().length];
                }
                ops[i] = op;
            }

            // add the response
            responses[i] = new BulkItemResponse(item.getId(), indexRequest.getOpType().lowercase(),
                    new IndexResponse(indexRequest.getIndex(), indexRequest.getType(), indexRequest.getId(), version));
        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < i; j++) {
                    applyVersion(request.getItems()[j], versions[j]);
                }
                throw (ElasticSearchException) e;
            }
            if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.getIndex(), shardRequest.shardId, indexRequest);
            } else {
                logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.getIndex(), shardRequest.shardId, indexRequest);
            }
            responses[i] = new BulkItemResponse(item.getId(), indexRequest.getOpType().lowercase(),
                    new BulkItemResponse.Failure(indexRequest.getIndex(), indexRequest.getType(), indexRequest.getId(), ExceptionsHelper.detailedMessage(e)));
            // nullify the request so it won't execute on the replicas
            request.getItems()[i] = null;
        }
    }

    @Override
    protected PrimaryResponse<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final BulkShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.getIndex()).shardSafe(shardRequest.shardId);

        Engine.IndexingOperation[] ops = null;

        Set<Tuple<String, String>> mappingsToUpdate = null;

        BulkItemResponse[] responses = new BulkItemResponse[request.getItems().length];
        long[] versions = new long[request.getItems().length];
        for (int i = 0; i < request.getItems().length; i++) {
            BulkItemRequest item = request.getItems()[i];
            if (item.getRequest() instanceof IndexRequest) {
            	
                indexRequest(clusterState, shardRequest, ops, versions, responses, 
                		mappingsToUpdate, item, indexShard, request, (IndexRequest) item.getRequest(), i);
                
            } else if(item.getRequest() instanceof PartialDocumentUpdateRequest) { 
            	PartialDocumentUpdateRequest updateRequest = (PartialDocumentUpdateRequest) item.getRequest();
            	final GetResult getResult = indexShard.getService().get(updateRequest.getType(), updateRequest.getId(),
                        new String[]{SourceFieldMapper.NAME, RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME}, true);

                // no doc, what to do, what to do...
                if (!getResult.isExists()) {
                    if (updateRequest.getUpsertRequest() == null) {
                        logger.trace("[{}][{}] failed to execute bulk item, document not indexed yet, " +
                        		"upsertRequest is missing (update) {}", shardRequest.request.getIndex(), shardRequest.shardId, updateRequest);
                        responses[i] = new BulkItemResponse(item.getId(), "update",
                                new BulkItemResponse.Failure(updateRequest.getIndex(), updateRequest.getType(), updateRequest.getId(), 
                                		"Document not indexed and upsertRequest is missing!"));
                        break;
                    }
                    
                    indexRequest(clusterState, shardRequest, ops, versions, responses, mappingsToUpdate, 
                    		item, indexShard, request, updateRequest.getUpsertRequest(), i);
                }
                
                if (getResult.internalSourceRef() == null) {
                	logger.trace("[{}][{}] failed to execute bulk item, indexed document without source (update) {}", shardRequest.request.getIndex(), shardRequest.shardId, updateRequest);
                    responses[i] = new BulkItemResponse(item.getId(), "update",
                            new BulkItemResponse.Failure(updateRequest.getIndex(), updateRequest.getType(), updateRequest.getId(), 
                            		"Indexed document without source!"));
                    break;
                }

                Tuple<XContentType, Map<String, Object>> sourceAndContent = XContentHelper.convertToMap(getResult.internalSourceRef(), true);
               // String operation = null;
               // String timestamp = null;
                Long ttl = null;
               // Object fetchedTTL = null;
                final Map<String, Object> updatedSourceAsMap;
                final XContentType updateSourceContentType = sourceAndContent.v1();
                String routing = getResult.getFields().containsKey(RoutingFieldMapper.NAME) ? getResult.field(RoutingFieldMapper.NAME).getValue().toString() : null;
                String parent = getResult.getFields().containsKey(ParentFieldMapper.NAME) ? getResult.field(ParentFieldMapper.NAME).getValue().toString() : null;

                IndexRequest indexRequest = updateRequest.getDoc();
                updatedSourceAsMap = sourceAndContent.v2();
                if (indexRequest.getTtl() > 0) {
                    ttl = indexRequest.getTtl();
                }
               // timestamp = indexRequest.getTimestamp();
                if (indexRequest.getRouting() != null) {
                    routing = indexRequest.getRouting();
                }
                if (indexRequest.getParent() != null) {
                    parent = indexRequest.getParent();
                }
                XContentHelper.update(updatedSourceAsMap, indexRequest.getSourceAsMap());
                
                XContentBuilder builder = null;
                try {
                    builder = XContentFactory.contentBuilder(updateSourceContentType);
                    builder.map(updatedSourceAsMap);
                } catch (IOException e) {
                    throw new ElasticSearchGenerationException("Failed to generate [" + updatedSourceAsMap + "]", e);
                }
                
                SourceToParse sourceToParse = SourceToParse.source(builder.bytes()).type(updateRequest.getType()).id(updateRequest.getId())
                        .routing(routing).parent(parent);
                
                Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.getVersion()).
                		versionType(indexRequest.getVersionType()).origin(Engine.Operation.Origin.PRIMARY);
                indexShard.index(index);
                
                // add the response
                responses[i] = new BulkItemResponse(item.getId(), "update",
                        new IndexResponse(updateRequest.getIndex(), updateRequest.getType(), updateRequest.getId(), indexRequest.getVersion()));
                
            	
            } else if (item.getRequest() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.getRequest();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.getType(), deleteRequest.getId(), deleteRequest.getVersion()).versionType(deleteRequest.getVersionType()).origin(Engine.Operation.Origin.PRIMARY);
                    indexShard.delete(delete);
                    // update the request with teh version so it will go to the replicas
                    deleteRequest.setVersion(delete.version());

                    // add the response
                    responses[i] = new BulkItemResponse(item.getId(), "delete",
                            new DeleteResponse(deleteRequest.getIndex(), deleteRequest.getType(), deleteRequest.getId(), delete.version(), delete.notFound()));
                } catch (Exception e) {
                    // rethrow the failure if we are going to retry on primary and let parent failure to handle it
                    if (retryPrimaryException(e)) {
                        // restore updated versions...
                        for (int j = 0; j < i; j++) {
                            applyVersion(request.getItems()[j], versions[j]);
                        }
                        throw (ElasticSearchException) e;
                    }
                    if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                        logger.trace("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.getIndex(), shardRequest.shardId, deleteRequest);
                    } else {
                        logger.debug("[{}][{}] failed to execute bulk item (delete) {}", e, shardRequest.request.getIndex(), shardRequest.shardId, deleteRequest);
                    }
                    responses[i] = new BulkItemResponse(item.getId(), "delete",
                            new BulkItemResponse.Failure(deleteRequest.getIndex(), deleteRequest.getType(), deleteRequest.getId(), ExceptionsHelper.detailedMessage(e)));
                    // nullify the request so it won't execute on the replicas
                    request.getItems()[i] = null;
                }
            }
        }

        if (mappingsToUpdate != null) {
            for (Tuple<String, String> mappingToUpdate : mappingsToUpdate) {
                updateMappingOnMaster(mappingToUpdate.v1(), mappingToUpdate.v2());
            }
        }

        if (request.isRefresh()) {
            try {
                indexShard.refresh(new Engine.Refresh(false));
            } catch (Exception e) {
                // ignore
            }
        }
        BulkShardResponse response = new BulkShardResponse(new ShardId(request.getIndex(), request.getShardId()), responses);
        return new PrimaryResponse<BulkShardResponse, BulkShardRequest>(shardRequest.request, response, ops);
    }

    @Override
    protected void postPrimaryOperation(BulkShardRequest request, PrimaryResponse<BulkShardResponse, BulkShardRequest> response) {
        IndexService indexService = indicesService.indexServiceSafe(request.getIndex());
        Engine.IndexingOperation[] ops = (Engine.IndexingOperation[]) response.payload();
        if (ops == null) {
            return;
        }
        for (int i = 0; i < ops.length; i++) {
            BulkItemRequest itemRequest = request.getItems()[i];
            BulkItemResponse itemResponse = response.response().getResponses()[i];
            if (itemResponse.isFailed()) {
                // failure, continue
                continue;
            }
            Engine.IndexingOperation op = ops[i];
            if (op == null) {
                continue; // failed / no matches requested
            }
            if (itemRequest.getRequest() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) itemRequest.getRequest();
                if (!Strings.hasLength(indexRequest.getPercolate())) {
                    continue;
                }
                try {
                    PercolatorExecutor.Response percolate = indexService.percolateService().percolate(new PercolatorExecutor.DocAndSourceQueryRequest(op.parsedDoc(), indexRequest.getPercolate()));
                    ((IndexResponse) itemResponse.getResponse()).setMatches(percolate.matches());
                } catch (Exception e) {
                    logger.warn("failed to percolate [{}]", e, itemRequest.getRequest());
                }
            }
        }
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.getIndex()).shardSafe(shardRequest.shardId);
        final BulkShardRequest request = shardRequest.request;
        for (int i = 0; i < request.getItems().length; i++) {
            BulkItemRequest item = request.getItems()[i];
            if (item == null) {
                continue;
            }
            if (item.getRequest() instanceof IndexRequest) {
                IndexRequest indexRequest = (IndexRequest) item.getRequest();
                try {
                    SourceToParse sourceToParse = SourceToParse.source(indexRequest.getSource()).type(indexRequest.getType()).id(indexRequest.getId())
                            .routing(indexRequest.getRouting()).parent(indexRequest.getParent()).timestamp(indexRequest.getTimestamp()).ttl(indexRequest.getTtl());

                    if (indexRequest.getOpType() == IndexRequest.OpType.INDEX) {
                        Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.getVersion()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.index(index);
                    } else {
                        Engine.Create create = indexShard.prepareCreate(sourceToParse).version(indexRequest.getVersion()).origin(Engine.Operation.Origin.REPLICA);
                        indexShard.create(create);
                    }
                } catch (Exception e) {
                    // ignore, we are on backup
                }
            } else if (item.getRequest() instanceof DeleteRequest) {
                DeleteRequest deleteRequest = (DeleteRequest) item.getRequest();
                try {
                    Engine.Delete delete = indexShard.prepareDelete(deleteRequest.getType(), deleteRequest.getId(), deleteRequest.getVersion()).origin(Engine.Operation.Origin.REPLICA);
                    indexShard.delete(delete);
                } catch (Exception e) {
                    // ignore, we are on backup
                }
            }
        }

        if (request.isRefresh()) {
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

    private void applyVersion(BulkItemRequest item, long version) {
        if (item.getRequest() instanceof IndexRequest) {
            ((IndexRequest) item.getRequest()).setVersion(version);
        } else if (item.getRequest() instanceof DeleteRequest) {
            ((DeleteRequest) item.getRequest()).setVersion(version);
        } else {
            // log?
        }
    }
}
