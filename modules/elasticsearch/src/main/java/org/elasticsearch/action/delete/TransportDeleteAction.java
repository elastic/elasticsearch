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

package org.elasticsearch.action.delete;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.delete.index.IndexDeleteRequest;
import org.elasticsearch.action.delete.index.IndexDeleteResponse;
import org.elasticsearch.action.delete.index.ShardDeleteResponse;
import org.elasticsearch.action.delete.index.TransportIndexDeleteAction;
import org.elasticsearch.action.support.replication.TransportShardReplicationOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 *
 * @author kimchy (shay.banon)
 */
public class TransportDeleteAction extends TransportShardReplicationOperationAction<DeleteRequest, DeleteResponse> {

    private final boolean autoCreateIndex;

    private final TransportCreateIndexAction createIndexAction;

    private final TransportIndexDeleteAction indexDeleteAction;

    @Inject public TransportDeleteAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                         IndicesService indicesService, ThreadPool threadPool, ShardStateAction shardStateAction,
                                         TransportCreateIndexAction createIndexAction, TransportIndexDeleteAction indexDeleteAction) {
        super(settings, transportService, clusterService, indicesService, threadPool, shardStateAction);
        this.createIndexAction = createIndexAction;
        this.indexDeleteAction = indexDeleteAction;
        this.autoCreateIndex = settings.getAsBoolean("action.auto_create_index", true);
    }

    @Override protected String executor() {
        return ThreadPool.Names.INDEX;
    }

    @Override protected void doExecute(final DeleteRequest deleteRequest, final ActionListener<DeleteResponse> listener) {
        if (autoCreateIndex && !clusterService.state().metaData().hasConcreteIndex(deleteRequest.index())) {
            createIndexAction.execute(new CreateIndexRequest(deleteRequest.index()), new ActionListener<CreateIndexResponse>() {
                @Override public void onResponse(CreateIndexResponse result) {
                    innerExecute(deleteRequest, listener);
                }

                @Override public void onFailure(Throwable e) {
                    if (ExceptionsHelper.unwrapCause(e) instanceof IndexAlreadyExistsException) {
                        // we have the index, do it
                        innerExecute(deleteRequest, listener);
                    } else {
                        listener.onFailure(e);
                    }
                }
            });
        } else {
            innerExecute(deleteRequest, listener);
        }
    }

    private void innerExecute(final DeleteRequest request, final ActionListener<DeleteResponse> listener) {
        ClusterState clusterState = clusterService.state();
        request.index(clusterState.metaData().concreteIndex(request.index())); // we need to get the concrete index here...
        if (clusterState.metaData().hasIndex(request.index())) {
            // check if routing is required, if so, do a broadcast delete
            MappingMetaData mappingMd = clusterState.metaData().index(request.index()).mapping(request.type());
            if (mappingMd != null && mappingMd.routing().required()) {
                if (request.routing() == null) {
                    indexDeleteAction.execute(new IndexDeleteRequest(request), new ActionListener<IndexDeleteResponse>() {
                        @Override public void onResponse(IndexDeleteResponse indexDeleteResponse) {
                            // go over the response, see if we have found one, and the version if found
                            long version = 0;
                            boolean found = false;
                            for (ShardDeleteResponse deleteResponse : indexDeleteResponse.responses()) {
                                if (!deleteResponse.notFound()) {
                                    found = true;
                                    version = deleteResponse.version();
                                    break;
                                }
                            }
                            listener.onResponse(new DeleteResponse(request.index(), request.type(), request.id(), version, !found));
                        }

                        @Override public void onFailure(Throwable e) {
                            listener.onFailure(e);
                        }
                    });
                    return;
                }
            }
        }
        super.doExecute(request, listener);
    }

    @Override protected boolean checkWriteConsistency() {
        return true;
    }

    @Override protected DeleteRequest newRequestInstance() {
        return new DeleteRequest();
    }

    @Override protected DeleteResponse newResponseInstance() {
        return new DeleteResponse();
    }

    @Override protected String transportAction() {
        return TransportActions.DELETE;
    }

    @Override protected void checkBlock(DeleteRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.WRITE, request.index());
    }

    @Override protected PrimaryResponse<DeleteResponse> shardOperationOnPrimary(ClusterState clusterState, ShardOperationRequest shardRequest) {
        DeleteRequest request = shardRequest.request;
        IndexShard indexShard = indexShard(shardRequest);
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version())
                .versionType(request.versionType())
                .origin(Engine.Operation.Origin.PRIMARY);
        indexShard.delete(delete);
        // update the request with teh version so it will go to the replicas
        request.version(delete.version());

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh(false));
            } catch (Exception e) {
                // ignore
            }
        }

        DeleteResponse response = new DeleteResponse(request.index(), request.type(), request.id(), delete.version(), delete.notFound());
        return new PrimaryResponse<DeleteResponse>(response, null);
    }

    @Override protected void shardOperationOnReplica(ShardOperationRequest shardRequest) {
        DeleteRequest request = shardRequest.request;
        IndexShard indexShard = indexShard(shardRequest);
        Engine.Delete delete = indexShard.prepareDelete(request.type(), request.id(), request.version())
                .origin(Engine.Operation.Origin.REPLICA);

        if (request.refresh()) {
            try {
                indexShard.refresh(new Engine.Refresh(false));
            } catch (Exception e) {
                // ignore
            }
        }

        indexShard.delete(delete);
    }

    @Override protected ShardIterator shards(ClusterState clusterState, DeleteRequest request) {
        return clusterService.operationRouting()
                .deleteShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing());
    }
}
