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

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 */
public class TransportDeleteAction extends TransportShardReplicationOperationAction<DeleteRequest, DeleteRequest, DeleteResponse> {

    private final AutoCreateIndex autoCreateIndex;
    private final TransportCreateIndexAction createIndexAction;

    @Inject
    public TransportDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                 IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                 TransportCreateIndexAction createIndexAction, ActionFilters actionFilters) {
        super(settings, DeleteAction.NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
                DeleteRequest.class, DeleteRequest.class, ThreadPool.Names.INDEX);
        this.createIndexAction = createIndexAction;
        this.autoCreateIndex = new AutoCreateIndex(settings);
    }

    @Override
    protected void doExecute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        if (autoCreateIndex.shouldAutoCreate(request.index(), clusterService.state())) {
            createIndexAction.execute(new CreateIndexRequest(request).index(request.index()).cause("auto(delete api)").masterNodeTimeout(request.timeout()), new ActionListener<CreateIndexResponse>() {
                @Override
                public void onResponse(CreateIndexResponse result) {
                    innerExecute(request, listener);
                }

                @Override
                public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        innerExecute(request, listener);
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
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected void resolveRequest(final ClusterState state, final InternalRequest request, final ActionListener<DeleteResponse> listener) {
        request.request().routing(state.metaData().resolveIndexRouting(request.request().routing(), request.request().index()));
        if (state.metaData().hasIndex(request.concreteIndex())) {
            // check if routing is required, if so, do a broadcast delete
            MappingMetaData mappingMd = state.metaData().index(request.concreteIndex()).mappingOrDefault(request.request().type());
            if (mappingMd != null && mappingMd.routing().required()) {
                if (request.request().routing() == null) {
                    if (request.request().versionType() != VersionType.INTERNAL) {
                        // TODO: implement this feature
                        throw new ElasticsearchIllegalArgumentException("routing value is required for deleting documents of type [" + request.request().type()
                                + "] while using version_type [" + request.request().versionType() + "]");
                    }
                    throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
                }
            }
        }
    }

    private void innerExecute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        super.doExecute(request, listener);
    }

    @Override
    protected boolean checkWriteConsistency() {
        return true;
    }

    @Override
    protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
    }

    @Override
    protected Tuple<DeleteResponse, DeleteRequest> shardOperationOnPrimary(ClusterState clusterState, PrimaryOperationRequest shardRequest) {
        DeleteRequest request = shardRequest.request;
        IndexShard indexShard = indicesService.indexServiceSafe(shardRequest.shardId.getIndex()).shardSafe(shardRequest.shardId.id());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), request.versionType(), Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with teh version so it will go to the replicas
        request.versionType(delete.versionType().versionTypeForReplicationAndRecovery());
        request.version(delete.version());

        assert request.versionType().validateVersionForWrites(request.version());

        if (request.refresh()) {
            try {
                indexShard.refresh("refresh_flag_delete");
            } catch (Exception e) {
                // ignore
            }
        }

        DeleteResponse response = new DeleteResponse(shardRequest.shardId.getIndex(), request.type(), request.id(), delete.version(), delete.found());
        return new Tuple<>(response, shardRequest.request);
    }

    @Override
    protected void shardOperationOnReplica(ShardId shardId, DeleteRequest request) {
        IndexShard indexShard = indicesService.indexServiceSafe(shardId.getIndex()).shardSafe(shardId.id());
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version(), request.versionType(), Engine.Operation.Origin.REPLICA);

        indexShard.delete(delete);

        if (request.refresh()) {
            try {
                indexShard.refresh("refresh_flag_delete");
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    protected ShardIterator shards(ClusterState clusterState, InternalRequest request) {
        return clusterService.operationRouting()
                .deleteShards(clusterService.state(), request.concreteIndex(), request.request().type(), request.request().id(), request.request().routing());
    }
}
