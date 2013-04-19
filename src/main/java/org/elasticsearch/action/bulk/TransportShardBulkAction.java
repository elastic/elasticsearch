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

import java.io.IOException;
import java.util.Map;
import java.util.Set;

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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.Engine;
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

import com.google.common.collect.Sets;

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
    
    private void indexRequest(ClusterState clusterState, PrimaryOperationRequest shardRequest,  Engine.IndexingOperation[] ops, long[] versions, BulkItemResponse[] responses, 
    		Set<Tuple<String, String>> mappingsToUpdate, BulkItemRequest item, IndexShard indexShard, BulkShardRequest request, IndexRequest indexRequest, int i) { 
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
            versions[i] = indexRequest.version();
            // update the version on request so it will happen on the replicas
            indexRequest.version(version);

            // update mapping on master if needed, we won't update changes to the same type, since once its changed, it won't have mappers added
            if (op.parsedDoc().mappingsModified()) {
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
            responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().lowercase(),
                    new IndexResponse(indexRequest.index(), indexRequest.type(), indexRequest.id(), version));
        } catch (Exception e) {
            // rethrow the failure if we are going to retry on primary and let parent failure to handle it
            if (retryPrimaryException(e)) {
                // restore updated versions...
                for (int j = 0; j < i; j++) {
                    applyVersion(request.items()[j], versions[j]);
                }
                throw (ElasticSearchException) e;
            }
            if (e instanceof ElasticSearchException && ((ElasticSearchException) e).status() == RestStatus.CONFLICT) {
                logger.trace("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
            } else {
                logger.debug("[{}][{}] failed to execute bulk item (index) {}", e, shardRequest.request.index(), shardRequest.shardId, indexRequest);
            }
            responses[i] = new BulkItemResponse(item.id(), indexRequest.opType().lowercase(),
                    new BulkItemResponse.Failure(indexRequest.index(), indexRequest.type(), indexRequest.id(), ExceptionsHelper.detailedMessage(e)));
            // nullify the request so it won't execute on the replicas
            request.items()[i] = null;
        }
    }

    @Override
    protected PrimaryResponse<BulkShardResponse, BulkShardRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) { 
        final BulkShardRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);

        Engine.IndexingOperation[] ops = null;

        Set<Tuple<String, String>> mappingsToUpdate = null;


        BulkItemResponse[] responses = new BulkItemResponse[request.items().length];
        long[] versions = new long[request.items().length];
        for (int i = 0; i < request.items().length; i++) {
            BulkItemRequest item = request.items()[i];
            if (item.request() instanceof IndexRequest) {
            	
                indexRequest(clusterState, shardRequest, ops, versions, responses, 
                		mappingsToUpdate, item, indexShard, request, (IndexRequest) item.request(), i);
                
            } else if(item.request() instanceof PartialDocumentUpdateRequest) { 
            	PartialDocumentUpdateRequest updateRequest = (PartialDocumentUpdateRequest) item.request();
            	final GetResult getResult = indexShard.getService().get(updateRequest.getType(), updateRequest.getId(),
                        new String[]{SourceFieldMapper.NAME, RoutingFieldMapper.NAME, ParentFieldMapper.NAME, TTLFieldMapper.NAME}, true);

                // no doc, what to do, what to do...
                if (!getResult.isExists()) {
                    if (updateRequest.getUpsertRequest() == null) {
                        logger.trace("[{}][{}] failed to execute bulk item, document not indexed yet, " +
                        		"upsertRequest is missing (update) {}", shardRequest.request.index(), shardRequest.shardId, updateRequest);
                        responses[i] = new BulkItemResponse(item.id(), "update",
                                new BulkItemResponse.Failure(updateRequest.index(), updateRequest.getType(), updateRequest.getId(), 
                                		"Document not indexed and upsertRequest is missing!"));
                        break;

                    }
                    
                    indexRequest(clusterState, shardRequest, ops, versions, responses, mappingsToUpdate, 
                    		item, indexShard, request, updateRequest.getUpsertRequest(), i);
                }
                
                if (getResult.internalSourceRef() == null) {
                	logger.trace("[{}][{}] failed to execute bulk item, indexed document without source (update) {}", shardRequest.request.index(), shardRequest.shardId, updateRequest);
                    responses[i] = new BulkItemResponse(item.id(), "update",
                            new BulkItemResponse.Failure(updateRequest.index(), updateRequest.getType(), updateRequest.getId(), 
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
                
                // set the version
                indexRequest.version(getResult.version()+1);
                
                updatedSourceAsMap = sourceAndContent.v2();
                if (indexRequest.ttl() > 0) {
                    ttl = indexRequest.ttl();
                }
               // timestamp = indexRequest.getTimestamp();
                if (indexRequest.routing() != null) {
                    routing = indexRequest.routing();
                }
                if (indexRequest.parent() != null) {
                    parent = indexRequest.parent();
                }
                XContentHelper.update(updatedSourceAsMap, indexRequest.sourceAsMap());
                
                XContentBuilder builder = null;
                try {
                    builder = XContentFactory.contentBuilder(updateSourceContentType);
                    builder.map(updatedSourceAsMap);
                } catch (IOException e) {
                    throw new ElasticSearchGenerationException("Failed to generate [" + updatedSourceAsMap + "]", e);
                }
                
                SourceToParse sourceToParse = SourceToParse.source(builder.bytes()).type(updateRequest.getType()).id(updateRequest.getId())
                        .routing(routing).parent(parent);
                
                Engine.Index index = indexShard.prepareIndex(sourceToParse).version(indexRequest.version()).
                		versionType(indexRequest.versionType()).origin(Engine.Operation.Origin.PRIMARY);
                indexShard.index(index);
                
                // add the response
                responses[i] = new BulkItemResponse(item.id(), "update",
                        new IndexResponse(updateRequest.index(), updateRequest.getType(), updateRequest.getId(), indexRequest.version()));
                
            	
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
                        // restore updated versions...
                        for (int j = 0; j < i; j++) {
                            applyVersion(request.items()[j], versions[j]);
                        }
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
                } catch (Exception e) {
                    logger.warn("failed to percolate [{}]", e, itemRequest.request());
                }
            }
        }
    }
    
    private void indexRequestOnReplica(IndexShard indexShard, IndexRequest indexRequest) { 
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
        	logger.warn("indexRequest on replica failed for request {}", e,indexRequest.id());
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
                
            	indexRequestOnReplica(indexShard, indexRequest);

                
            } else if(item.request() instanceof PartialDocumentUpdateRequest) {
            	PartialDocumentUpdateRequest updateRequest = (PartialDocumentUpdateRequest) item.request();
            	logger.debug("updateRequest on replica for request {}",updateRequest.getId());
            	
            	// document is already merged in shardOperationOnPrimary action
            	
            	IndexRequest indexRequest = updateRequest.getUpsertRequest();
            	if(indexRequest == null) { 
            		indexRequest = updateRequest.getDoc();
            	}
            	indexRequestOnReplica(indexShard, indexRequest);
            	
            }  else if (item.request() instanceof DeleteRequest) {
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
