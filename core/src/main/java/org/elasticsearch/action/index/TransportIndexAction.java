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

package org.elasticsearch.action.index;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the index operation.
 *
 * Allows for the following settings:
 * <ul>
 * <li><b>autoCreateIndex</b>: When set to <tt>true</tt>, will automatically create an index if one does not exists.
 * Defaults to <tt>true</tt>.
 * <li><b>allowIdGeneration</b>: If the id is set not, should it be generated. Defaults to <tt>true</tt>.
 * </ul>
 */
public class TransportIndexAction extends TransportReplicationAction<IndexRequest, IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final boolean allowIdGeneration;
    private final TransportCreateIndexAction createIndexAction;

    private final ClusterService clusterService;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction,
                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                AutoCreateIndex autoCreateIndex) {
        super(settings, IndexAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, mappingUpdatedAction,
                actionFilters, indexNameExpressionResolver, IndexRequest::new, IndexRequest::new, ThreadPool.Names.INDEX);
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        ClusterState state = clusterService.state();
        if (autoCreateIndex.shouldAutoCreate(request.index(), state)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(request);
            createIndexRequest.index(request.index());
            createIndexRequest.mapping(request.type());
            createIndexRequest.cause("auto(index api)");
            createIndexRequest.masterNodeTimeout(request.timeout());
            createIndexAction.execute(createIndexRequest, new ActionListener<CreateIndexResponse>() {
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
    protected void resolveRequest(MetaData metaData, String concreteIndex, IndexRequest request) {
        MappingMetaData mappingMd = null;
        if (metaData.hasIndex(concreteIndex)) {
            mappingMd = metaData.index(concreteIndex).mappingOrDefault(request.type());
        }
        request.process(metaData, mappingMd, allowIdGeneration, concreteIndex);
        ShardId shardId = clusterService.operationRouting().shardId(clusterService.state(), concreteIndex, request.id(), request.routing());
        request.setShardId(shardId);
    }

    private void innerExecute(final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected Tuple<IndexResponse, IndexRequest> shardOperationOnPrimary(MetaData metaData, IndexRequest request) throws Throwable {

        // validate, if routing is required, that we got routing
        IndexMetaData indexMetaData = metaData.index(request.shardId().getIndex());
        MappingMetaData mappingMd = indexMetaData.mappingOrDefault(request.type());
        if (mappingMd != null && mappingMd.routing().required()) {
            if (request.routing() == null) {
                throw new RoutingMissingException(request.shardId().getIndex(), request.type(), request.id());
            }
        }

        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());

        final WriteResult<IndexResponse> result = executeIndexRequestOnPrimary(request, indexShard, mappingUpdatedAction);

        final IndexResponse response = result.response;
        final Translog.Location location = result.location;
        processAfterWrite(request.refresh(), indexShard, location);
        return new Tuple<>(response, request);
    }

    @Override
    protected void shardOperationOnReplica(IndexRequest request) {
        final ShardId shardId = request.shardId();
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        final Engine.Index operation = executeIndexRequestOnReplica(request, indexShard);
        processAfterWrite(request.refresh(), indexShard, operation.getTranslogLocation());
    }

    /**
     * Execute the given {@link IndexRequest} on a replica shard, throwing a
     * {@link RetryOnReplicaException} if the operation needs to be re-tried.
     */
    public static Engine.Index executeIndexRequestOnReplica(IndexRequest request, IndexShard indexShard) {
        final ShardId shardId = indexShard.shardId();
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, request.source()).index(shardId.getIndex()).type(request.type()).id(request.id())
                .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());

        final Engine.Index operation = indexShard.prepareIndexOnReplica(sourceToParse, request.version(), request.versionType());
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
        }
        indexShard.index(operation);
        return operation;
    }

    /** Utility method to prepare an index operation on primary shards */
    public static Engine.Index prepareIndexOperationOnPrimary(IndexRequest request, IndexShard indexShard) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.source()).index(request.index()).type(request.type()).id(request.id())
            .routing(request.routing()).parent(request.parent()).timestamp(request.timestamp()).ttl(request.ttl());
        return indexShard.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType());
    }

    /**
     * Execute the given {@link IndexRequest} on a primary shard, throwing a
     * {@link RetryOnPrimaryException} if the operation needs to be re-tried.
     */
    public static WriteResult<IndexResponse> executeIndexRequestOnPrimary(IndexRequest request, IndexShard indexShard, MappingUpdatedAction mappingUpdatedAction) throws Throwable {
        Engine.Index operation = prepareIndexOperationOnPrimary(request, indexShard);
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = indexShard.shardId();
        if (update != null) {
            final String indexName = shardId.getIndex();
            mappingUpdatedAction.updateMappingOnMasterSynchronously(indexName, request.type(), update);
            operation = prepareIndexOperationOnPrimary(request, indexShard);
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new RetryOnPrimaryException(shardId,
                    "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }
        final boolean created = indexShard.index(operation);

        // update the version on request so it will happen on the replicas
        final long version = operation.version();
        request.version(version);
        request.versionType(request.versionType().versionTypeForReplicationAndRecovery());

        assert request.versionType().validateVersionForWrites(request.version());

        return new WriteResult<>(new IndexResponse(shardId, request.type(), request.id(), request.version(), created), operation.getTranslogLocation());
    }

}

