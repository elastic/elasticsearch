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

package org.elasticsearch.action.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Performs the index operation.
 * <p/>
 * <p>Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to <tt>true</tt>, will automatically create an index if one does not exists.
 * Defaults to <tt>true</tt>.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to <tt>true</tt>.
 * </ul>
 */
public class TransportIndexAction extends TransportShardReplicationOperationAction<IndexRequest, IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;

    private final boolean allowIdGeneration;

    private final TransportCreateIndexAction createIndexAction;

    private final MappingUpdatedAction mappingUpdatedAction;

    private final boolean waitForMappingChange;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.autoCreateIndex = new AutoCreateIndex(settings);
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.waitForMappingChange = settings.getAsBoolean("action.wait_on_mapping_change", false);
    }

    @Override
    protected void doExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            request.beforeLocalFork(); // we fork on another thread...
            createIndexAction.execute(new CreateIndexRequest(request.index()).cause("auto(index api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(request, listener);
                        } catch (Throwable e1) {
                            listener.onFailure(e1);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(request, listener);
        }
    }

    @Override
    protected boolean resolveRequest(ClusterState state, IndexRequest request, ActionListener<IndexResponse> indexResponseActionListener) {
        MetaData metaData = clusterService.state().metaData();
        String aliasOrIndex = request.index();
        request.index(metaData.concreteIndex(request.index()));
        MappingMetaData mappingMd = null;
        if (metaData.hasIndex(request.index())) {
            mappingMd = metaData.index(request.index()).mappingOrDefault(request.type());
        }
        request.process(metaData, aliasOrIndex, mappingMd, allowIdGeneration);
        return true;
    }

    private void innerExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected IndexRequest newRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexRequest newReplicaRequestInstance() {
        return new IndexRequest();
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected String transportAction() {
        return IndexAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndexRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndexRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, IndexRequest request) {
        return clusterService.operationRouting()
                .indexShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
    }

    @Override
    protected PrimaryResponse<IndexResponse, IndexRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        final IndexRequest request = shardRequest.request;

        // validate, if routing is required, that we got routing
        IndexMetaData indexMetaData = clusterState.metaData().index(request.index());
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (request.routing() == null) {
                throw new RoutingMissingException(request.index(), request.type(), request.id());
            }
        }

        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        long version;
        Engine.IndexingOperation op;
        if (request.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse)
                    .version(request.version())
                    .versionType(request.versionType())
                    .origin(Engine.Operation.Origin.PRIMARY);
            if (index.parsedDoc().mappingsModified()) {
                updateMappingOnMaster(request, indexMetaData);
            }
            indexShard.index(index);
            version = index.version();
            op = index;
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse)
                    .version(request.version())
                    .versionType(request.versionType())
                    .origin(Engine.Operation.Origin.PRIMARY);
            if (create.parsedDoc().mappingsModified()) {
                updateMappingOnMaster(request, indexMetaData);
            }
            indexShard.create(create);
            version = create.version();
            op = create;
        }
        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_index").force(false));
            } catch (Throwable e) {
                // ignore
            }
        }

        // update the version on the request, so it will be used for the replicas
        request.version(version);

        IndexResponse response = new IndexResponse(request.index(), request.type(), request.id(), version);
        return new PrimaryResponse<IndexResponse, IndexRequest>(shardRequest.request, response, op);
    }

    @Override
    protected void postPrimaryOperation(IndexRequest request, PrimaryResponse<IndexResponse, IndexRequest> response) {
        Engine.IndexingOperation op = (Engine.IndexingOperation) response.payload();
        if (!Strings.hasLength(request.percolate())) {
            return;
        }
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        try {
            PercolatorExecutor.Response percolate = indexService.percolateService().percolate(new PercolatorExecutor.DocAndSourceQueryRequest(op.parsedDoc(), request.percolate()));
            response.response().setMatches(percolate.matches());
        } catch (Exception e) {
            logger.warn("failed to percolate [{}]", e, request);
        }
    }

    @Override
    protected void shardOperationOnReplica(ReplicaOperationRequest shardRequest) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.request.index()).shardSafe(shardRequest.shardId);
        IndexRequest request = shardRequest.request;
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, request.source()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        if (request.opType() == IndexRequest.OpType.INDEX) {
            Engine.Index index = indexShard.prepareIndex(sourceToParse)
                    .version(request.version())
                    .origin(Engine.Operation.Origin.REPLICA);
            indexShard.index(index);
        } else {
            Engine.Create create = indexShard.prepareCreate(sourceToParse)
                    .version(request.version())
                    .origin(Engine.Operation.Origin.REPLICA);
            indexShard.create(create);
        }
        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh("refresh_flag_index").force(false));
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void updateMappingOnMaster(final IndexRequest request, IndexMetaData indexMetaData) {
        final CountDownLatch latch = new CountDownLatch(1);
        try {
            final MapperService mapperService = indicesService.indexServiceSafe(request.index()).mapperService();
            final DocumentMapper documentMapper = mapperService.documentMapper(request.type());
            if (documentMapper == null) { // should not happen
                return;
            }
            // we generate the order id before we get the mapping to send and refresh the source, so
            // if 2 happen concurrently, we know that the later order will include the previous one
            long orderId = mappingUpdatedAction.generateNextMappingUpdateOrder();
            documentMapper.refreshSource();
            DiscoveryNode node = clusterService.localNode();
            final MappingUpdatedAction.MappingUpdatedRequest mappingRequest =
                    new MappingUpdatedAction.MappingUpdatedRequest(request.index(), indexMetaData.uuid(), request.type(), documentMapper.mappingSource(), orderId, node != null ? node.id() : null);
            logger.trace("Sending mapping updated to master: {}", mappingRequest);
            mappingUpdatedAction.execute(mappingRequest, new ActionListener<MappingUpdatedAction.MappingUpdatedResponse>() {
                @Override
                public void onResponse(MappingUpdatedAction.MappingUpdatedResponse mappingUpdatedResponse) {
                    // all is well
                    latch.countDown();
                }

                @Override
                public void onFailure(Throwable e) {
                    latch.countDown();
                    logger.warn("Failed to update master on updated mapping for {}", e, mappingRequest);
                }
            });
        } catch (Exception e) {
            latch.countDown();
            logger.warn("Failed to update master on updated mapping for index [" + request.index() + "], type [" + request.type() + "]", e);
        }

        if (waitForMappingChange) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }
}
