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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.index.MappingUpdatedAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
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
public class TransportIndexAction extends TransportWriteAction<IndexRequest, IndexRequest, IndexResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final boolean allowIdGeneration;
    private final TransportCreateIndexAction createIndexAction;

    private final ClusterService clusterService;
    private final MappingUpdatedAction mappingUpdatedAction;

    @Inject
    public TransportIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                TransportCreateIndexAction createIndexAction, MappingUpdatedAction mappingUpdatedAction,
                                ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                AutoCreateIndex autoCreateIndex) {
        super(settings, IndexAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction,
            actionFilters, indexNameExpressionResolver, IndexRequest::new, IndexRequest::new, ThreadPool.Names.INDEX);
        this.mappingUpdatedAction = mappingUpdatedAction;
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
        this.allowIdGeneration = settings.getAsBoolean("action.allow_id_generation", true);
        this.clusterService = clusterService;
    }

    @Override
    protected void doExecute(Task task, final IndexRequest request, final ActionListener<IndexResponse> listener) {
        // if we don't have a master, we don't have metadata, that's fine, let it find a master using create index API
        ClusterState state = clusterService.state();
        if (autoCreateIndex.shouldAutoCreate(request.index(), state)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest();
            createIndexRequest.index(request.index());
            createIndexRequest.cause("auto(index api)");
            createIndexRequest.masterNodeTimeout(request.timeout());
            createIndexAction.execute(task, createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(task, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        try {
                            innerExecute(task, request, listener);
                        } catch (Exception inner) {
                            inner.addSuppressed(e);
                            listener.onFailure(inner);
                        }
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(task, request, listener);
        }
    }

    @Override
    protected void resolveRequest(MetaData metaData, IndexMetaData indexMetaData, IndexRequest request) {
        super.resolveRequest(metaData, indexMetaData, request);
        MappingMetaData mappingMd =indexMetaData.mappingOrDefault(request.type());
        request.resolveRouting(metaData);
        request.process(mappingMd, allowIdGeneration, indexMetaData.getIndex().getName());
        ShardId shardId = clusterService.operationRouting().shardId(clusterService.state(),
            indexMetaData.getIndex().getName(), request.id(), request.routing());
        request.setShardId(shardId);
    }

    private void innerExecute(Task task, final IndexRequest request, final ActionListener<IndexResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected IndexResponse newResponseInstance() {
        return new IndexResponse();
    }

    @Override
    protected WritePrimaryResult shardOperationOnPrimary(IndexRequest request, IndexShard primary) throws Exception {
        final Engine.IndexResult indexResult = executeIndexRequestOnPrimary(request, primary, mappingUpdatedAction);
        final IndexResponse response;
        if (indexResult.hasFailure() == false) {
            // update the version on request so it will happen on the replicas
            final long version = indexResult.getVersion();
            request.version(version);
            request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
            request.seqNo(indexResult.getSeqNo());
            assert request.versionType().validateVersionForWrites(request.version());
            response = new IndexResponse(primary.shardId(), request.type(), request.id(), indexResult.getSeqNo(),
                    indexResult.getVersion(), indexResult.isCreated());
        } else {
            response = null;
        }
        return new WritePrimaryResult(request, response, indexResult.getTranslogLocation(), indexResult.getFailure(), primary);
    }

    @Override
    protected WriteReplicaResult shardOperationOnReplica(IndexRequest request, IndexShard replica) throws Exception {
        final Engine.IndexResult indexResult = executeIndexRequestOnReplica(request, replica);
        return new WriteReplicaResult(request, indexResult.getTranslogLocation(), indexResult.getFailure(), replica);
    }

    /**
     * Execute the given {@link IndexRequest} on a replica shard, throwing a
     * {@link RetryOnReplicaException} if the operation needs to be re-tried.
     */
    public static Engine.IndexResult executeIndexRequestOnReplica(IndexRequest request, IndexShard replica) {
        final ShardId shardId = replica.shardId();
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.REPLICA, shardId.getIndexName(), request.type(), request.id(), request.source())
                .routing(request.routing()).parent(request.parent());

        final Engine.Index operation;
        try {
            operation = replica.prepareIndexOnReplica(sourceToParse, request.seqNo(), request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
        } catch (MapperParsingException e) {
            return new Engine.IndexResult(e, request.version(), request.seqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        if (update != null) {
            throw new RetryOnReplicaException(shardId, "Mappings are not available on the replica yet, triggered update: " + update);
        }
        return replica.index(operation);
    }

    /** Utility method to prepare an index operation on primary shards */
    static Engine.Index prepareIndexOperationOnPrimary(IndexRequest request, IndexShard primary) {
        SourceToParse sourceToParse = SourceToParse.source(SourceToParse.Origin.PRIMARY, request.index(), request.type(), request.id(), request.source())
            .routing(request.routing()).parent(request.parent());
        return primary.prepareIndexOnPrimary(sourceToParse, request.version(), request.versionType(), request.getAutoGeneratedTimestamp(), request.isRetry());
    }

    public static Engine.IndexResult executeIndexRequestOnPrimary(IndexRequest request, IndexShard primary,
            MappingUpdatedAction mappingUpdatedAction) throws Exception {
        Engine.Index operation;
        try {
            operation = prepareIndexOperationOnPrimary(request, primary);
        } catch (MapperParsingException | IllegalArgumentException e) {
            return new Engine.IndexResult(e, request.version(), request.seqNo());
        }
        Mapping update = operation.parsedDoc().dynamicMappingsUpdate();
        final ShardId shardId = primary.shardId();
        if (update != null) {
            // can throw timeout exception when updating mappings or ISE for attempting to update default mappings
            // which are bubbled up
            try {
                mappingUpdatedAction.updateMappingOnMaster(shardId.getIndex(), request.type(), update);
            } catch (IllegalArgumentException e) {
                // throws IAE on conflicts merging dynamic mappings
                return new Engine.IndexResult(e, request.version(), request.seqNo());
            }
            try {
                operation = prepareIndexOperationOnPrimary(request, primary);
            } catch (MapperParsingException | IllegalArgumentException e) {
                return new Engine.IndexResult(e, request.version(), request.seqNo());
            }
            update = operation.parsedDoc().dynamicMappingsUpdate();
            if (update != null) {
                throw new ReplicationOperation.RetryOnPrimaryException(shardId,
                    "Dynamic mappings are not available on the node that holds the primary yet");
            }
        }

        return primary.index(operation);
    }

}

