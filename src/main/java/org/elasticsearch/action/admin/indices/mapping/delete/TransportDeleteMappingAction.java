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

package org.elasticsearch.action.admin.indices.mapping.delete;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.IndexDeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.QuerySourceBuilder;
import org.elasticsearch.action.support.broadcast.BroadcastOperationResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TypeFilterBuilder;
import org.elasticsearch.indices.TypeMissingException;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Set;

/**
 * Delete mapping action.
 */
public class TransportDeleteMappingAction extends TransportMasterNodeOperationAction<DeleteMappingRequest, DeleteMappingResponse> {

    private final MetaDataMappingService metaDataMappingService;
    private final TransportFlushAction flushAction;
    private final TransportDeleteByQueryAction deleteByQueryAction;
    private final TransportRefreshAction refreshAction;
    private final DestructiveOperations destructiveOperations;

    @Inject
    public TransportDeleteMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, MetaDataMappingService metaDataMappingService,
                                        TransportDeleteByQueryAction deleteByQueryAction, TransportRefreshAction refreshAction,
                                        TransportFlushAction flushAction, NodeSettingsService nodeSettingsService) {
        super(settings, DeleteMappingAction.NAME, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
        this.deleteByQueryAction = deleteByQueryAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
        this.destructiveOperations = new DestructiveOperations(logger, settings, nodeSettingsService);
    }

    @Override
    protected String executor() {
        // no need for fork on another thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected DeleteMappingRequest newRequest() {
        return new DeleteMappingRequest();
    }

    @Override
    protected DeleteMappingResponse newResponse() {
        return new DeleteMappingResponse();
    }

    @Override
    protected void doExecute(DeleteMappingRequest request, ActionListener<DeleteMappingResponse> listener) {
        destructiveOperations.failDestructive(request.indices());
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteMappingRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    @Override
    protected void masterOperation(final DeleteMappingRequest request, final ClusterState state, final ActionListener<DeleteMappingResponse> listener) throws ElasticsearchException {
        final String[] concreteIndices = state.metaData().concreteIndices(request.indicesOptions(), request.indices());
        flushAction.execute(Requests.flushRequest(concreteIndices), new ActionListener<FlushResponse>() {
            @Override
            public void onResponse(FlushResponse flushResponse) {
                if (logger.isTraceEnabled()) {
                    traceLogResponse("Flush", flushResponse);
                }
  
                // get all types that need to be deleted.
                ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> result = clusterService.state().metaData().findMappings(
                        concreteIndices, request.types()
                );
                // create OrFilter with type filters within to account for different types
                BoolFilterBuilder filterBuilder = new BoolFilterBuilder();
                Set<String> types = new HashSet<>();
                for (ObjectObjectCursor<String, ImmutableOpenMap<String, MappingMetaData>> typesMeta : result) {
                    for (ObjectObjectCursor<String, MappingMetaData> type : typesMeta.value) {
                        filterBuilder.should(new TypeFilterBuilder(type.key));
                        types.add(type.key);
                    }
                }
                if (types.size() == 0) {
                    throw new TypeMissingException(new Index("_all"), request.types(), "No index has the type.");
                }
                request.types(types.toArray(new String[types.size()]));
                QuerySourceBuilder querySourceBuilder = new QuerySourceBuilder()
                        .setQuery(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder));
                deleteByQueryAction.execute(Requests.deleteByQueryRequest(concreteIndices).types(request.types()).source(querySourceBuilder), new ActionListener<DeleteByQueryResponse>() {
                    @Override
                    public void onResponse(DeleteByQueryResponse deleteByQueryResponse) {
                        if (logger.isTraceEnabled()) {
                            for (IndexDeleteByQueryResponse indexResponse : deleteByQueryResponse) {
                                logger.trace("Delete by query[{}] completed with total[{}], successful[{}] and failed[{}]", indexResponse.getIndex(), indexResponse.getTotalShards(), indexResponse.getSuccessfulShards(), indexResponse.getFailedShards());
                                if (indexResponse.getFailedShards() > 0) {
                                    for (ShardOperationFailedException failure : indexResponse.getFailures()) {
                                        logger.trace("[{}/{}] Delete by query shard failure reason: {}", failure.index(), failure.shardId(), failure.reason());
                                    }
                                }
                            }
                        }
                        refreshAction.execute(Requests.refreshRequest(concreteIndices), new ActionListener<RefreshResponse>() {
                            @Override
                            public void onResponse(RefreshResponse refreshResponse) {
                                if (logger.isTraceEnabled()) {
                                    traceLogResponse("Refresh", refreshResponse);
                                }
                                removeMapping();
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                if (logger.isDebugEnabled()) {
                                    logger.debug("Refresh failed completely", e);
                                }
                                removeMapping();
                            }

                            protected void removeMapping() {
                                DeleteMappingClusterStateUpdateRequest clusterStateUpdateRequest = new DeleteMappingClusterStateUpdateRequest()
                                        .indices(concreteIndices).types(request.types())
                                        .ackTimeout(request.timeout())
                                        .masterNodeTimeout(request.masterNodeTimeout());

                                metaDataMappingService.removeMapping(clusterStateUpdateRequest, new ActionListener<ClusterStateUpdateResponse>() {
                                    @Override
                                    public void onResponse(ClusterStateUpdateResponse response) {
                                        listener.onResponse(new DeleteMappingResponse(response.isAcknowledged()));
                                    }

                                    @Override
                                    public void onFailure(Throwable t) {
                                        listener.onFailure(t);
                                    }
                                });
                            }
                        });
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        listener.onFailure(t);
                    }
                });
            }

            @Override
            public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    private void traceLogResponse(String action, BroadcastOperationResponse response) {
        logger.trace("{} completed with total[{}], successful[{}] and failed[{}]", action, response.getTotalShards(), response.getSuccessfulShards(), response.getFailedShards());
        if (response.getFailedShards() > 0) {
            for (ShardOperationFailedException failure : response.getShardFailures()) {
                logger.trace("[{}/{}] {} shard failure reason: {}", failure.index(), failure.shardId(), action, failure.reason());
            }
        }
    }
}
