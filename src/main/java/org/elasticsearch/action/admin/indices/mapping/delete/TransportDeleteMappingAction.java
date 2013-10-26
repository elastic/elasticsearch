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

package org.elasticsearch.action.admin.indices.mapping.delete;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.TransportFlushAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.refresh.TransportRefreshAction;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.action.deletebyquery.TransportDeleteByQueryAction;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataMappingService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete mapping action.
 */
public class TransportDeleteMappingAction extends TransportMasterNodeOperationAction<DeleteMappingRequest, DeleteMappingResponse> {

    private final MetaDataMappingService metaDataMappingService;
    private final TransportFlushAction flushAction;
    private final TransportDeleteByQueryAction deleteByQueryAction;
    private final TransportRefreshAction refreshAction;

    @Inject
    public TransportDeleteMappingAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool, MetaDataMappingService metaDataMappingService,
                                        TransportDeleteByQueryAction deleteByQueryAction, TransportRefreshAction refreshAction, TransportFlushAction flushAction) {
        super(settings, transportService, clusterService, threadPool);
        this.metaDataMappingService = metaDataMappingService;
        this.deleteByQueryAction = deleteByQueryAction;
        this.refreshAction = refreshAction;
        this.flushAction = flushAction;
    }

    @Override
    protected String executor() {
        // no need for fork on another thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return DeleteMappingAction.NAME;
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
        // update to concrete indices
        request.indices(clusterService.state().metaData().concreteIndices(request.indices()));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteMappingRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected void masterOperation(final DeleteMappingRequest request, final ClusterState state, final ActionListener<DeleteMappingResponse> listener) throws ElasticSearchException {
        flushAction.execute(Requests.flushRequest(request.indices()), new ActionListener<FlushResponse>() {
            @Override
            public void onResponse(FlushResponse flushResponse) {
                deleteByQueryAction.execute(Requests.deleteByQueryRequest(request.indices()).query(QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), FilterBuilders.typeFilter(request.type()))), new ActionListener<DeleteByQueryResponse>() {
                    @Override
                    public void onResponse(DeleteByQueryResponse deleteByQueryResponse) {
                        refreshAction.execute(Requests.refreshRequest(request.indices()), new ActionListener<RefreshResponse>() {
                            @Override
                            public void onResponse(RefreshResponse refreshResponse) {
                                removeMapping();
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                removeMapping();
                            }

                            protected void removeMapping() {
                                DeleteMappingClusterStateUpdateRequest clusterStateUpdateRequest = new DeleteMappingClusterStateUpdateRequest()
                                        .indices(request.indices()).type(request.type())
                                        .ackTimeout(request.timeout())
                                        .masterNodeTimeout(request.masterNodeTimeout());

                                metaDataMappingService.removeMapping(clusterStateUpdateRequest, new ClusterStateUpdateListener() {
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
}
