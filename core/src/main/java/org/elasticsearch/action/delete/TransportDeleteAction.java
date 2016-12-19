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

package org.elasticsearch.action.delete;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 */
public class TransportDeleteAction extends TransportWriteAction<DeleteRequest, DeleteRequest,DeleteResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                 IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                 TransportCreateIndexAction createIndexAction, ActionFilters actionFilters,
                                 IndexNameExpressionResolver indexNameExpressionResolver,
                                 AutoCreateIndex autoCreateIndex) {
        super(settings, DeleteAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                indexNameExpressionResolver, DeleteRequest::new, DeleteRequest::new, ThreadPool.Names.INDEX);
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = autoCreateIndex;
    }

    @Override
    protected void doExecute(Task task, final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        ClusterState state = clusterService.state();
        if (autoCreateIndex.shouldAutoCreate(request.index(), state)) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest()
                    .index(request.index())
                    .cause("auto(delete api)")
                    .masterNodeTimeout(request.timeout());
            createIndexAction.execute(task, createIndexRequest, new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(task, request, listener);
                }

                @Override
                public void onFailure(Exception e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof ResourceAlreadyExistsException) {
                        // we have the index, do it
                        innerExecute(task, request, listener);
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
    protected void resolveRequest(final MetaData metaData, IndexMetaData indexMetaData, DeleteRequest request) {
        super.resolveRequest(metaData, indexMetaData, request);
        resolveAndValidateRouting(metaData, indexMetaData.getIndex().getName(), request);
        ShardId shardId = clusterService.operationRouting().shardId(clusterService.state(),
            indexMetaData.getIndex().getName(), request.id(), request.routing());
        request.setShardId(shardId);
    }

    public static void resolveAndValidateRouting(final MetaData metaData, final String concreteIndex,
                                                 DeleteRequest request) {
        request.routing(metaData.resolveIndexRouting(request.parent(), request.routing(), request.index()));
        // check if routing is required, if so, throw error if routing wasn't specified
        if (request.routing() == null && metaData.routingRequired(concreteIndex, request.type())) {
            throw new RoutingMissingException(concreteIndex, request.type(), request.id());
        }
    }

    private void innerExecute(Task task, final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        super.doExecute(task, request, listener);
    }

    @Override
    protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
    }

    @Override
    protected WritePrimaryResult shardOperationOnPrimary(DeleteRequest request, IndexShard primary) throws Exception {
        final Engine.DeleteResult result = executeDeleteRequestOnPrimary(request, primary);
        final DeleteResponse response;
        final DeleteRequest replicaRequest;
        if (result.hasFailure() == false) {
            // update the request with the version so it will go to the replicas
            request.versionType(request.versionType().versionTypeForReplicationAndRecovery());
            request.version(result.getVersion());
            request.setSeqNo(result.getSeqNo());
            assert request.versionType().validateVersionForWrites(request.version());
            replicaRequest = request;
            response = new DeleteResponse(
                primary.shardId(),
                request.type(),
                request.id(),
                result.getSeqNo(),
                result.getVersion(),
                result.isFound());
        } else {
            response = null;
            replicaRequest = null;
        }
        return new WritePrimaryResult(replicaRequest, response, result.getTranslogLocation(), result.getFailure(), primary);
    }

    @Override
    protected WriteReplicaResult shardOperationOnReplica(DeleteRequest request, IndexShard replica) throws Exception {
        final Engine.DeleteResult result = executeDeleteRequestOnReplica(request, replica);
        return new WriteReplicaResult(request, result.getTranslogLocation(), result.getFailure(), replica);
    }


    public static Engine.DeleteResult executeDeleteRequestOnPrimary(DeleteRequest request, IndexShard primary) {
        final Engine.Delete delete = primary.prepareDeleteOnPrimary(request.type(), request.id(), request.version(), request.versionType());
        return primary.delete(delete);
    }

    public static Engine.DeleteResult executeDeleteRequestOnReplica(DeleteRequest request, IndexShard replica) {
        final Engine.Delete delete = replica.prepareDeleteOnReplica(request.type(), request.id(),
                request.getSeqNo(), request.primaryTerm(), request.version(), request.versionType());
        return replica.delete(delete);
    }

}
