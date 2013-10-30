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

package org.elasticsearch.action.admin.indices.warmer.put;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportSearchAction;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.ClusterStateUpdateListener;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataWarmersService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Arrays;

/**
 * Put warmer action.
 */
public class TransportPutWarmerAction extends TransportMasterNodeOperationAction<PutWarmerRequest, PutWarmerResponse> {

    private final TransportSearchAction searchAction;

    private final MetaDataWarmersService metaDataWarmersService;

    @Inject
    public TransportPutWarmerAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    TransportSearchAction searchAction, MetaDataWarmersService metaDataWarmersService) {
        super(settings, transportService, clusterService, threadPool);
        this.searchAction = searchAction;
        this.metaDataWarmersService = metaDataWarmersService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return PutWarmerAction.NAME;
    }

    @Override
    protected PutWarmerRequest newRequest() {
        return new PutWarmerRequest();
    }

    @Override
    protected PutWarmerResponse newResponse() {
        return new PutWarmerResponse();
    }

    @Override
    protected ClusterBlockException checkBlock(PutWarmerRequest request, ClusterState state) {
        String[] concreteIndices = clusterService.state().metaData().concreteIndices(request.searchRequest().indices());
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    @Override
    protected void masterOperation(final PutWarmerRequest request, final ClusterState state, final ActionListener<PutWarmerResponse> listener) throws ElasticSearchException {
        // first execute the search request, see that its ok...
        searchAction.execute(request.searchRequest(), new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                if (searchResponse.getFailedShards() > 0) {
                    listener.onFailure(new ElasticSearchException("search failed with failed shards: " + Arrays.toString(searchResponse.getShardFailures())));
                    return;
                }

                PutWarmerClusterStateUpdateRequest putWarmerRequest = new PutWarmerClusterStateUpdateRequest(request.name())
                        .types(request.searchRequest().types())
                        .indices(request.searchRequest().indices())
                        .ackTimeout(request.timeout()).masterNodeTimeout(request.masterNodeTimeout());

                if (request.searchRequest().source() != null && request.searchRequest().source().length() > 0) {
                    putWarmerRequest.source(request.searchRequest().source());
                } else if (request.searchRequest().extraSource() != null && request.searchRequest().extraSource().length() > 0) {
                    putWarmerRequest.source(request.searchRequest().extraSource());
                }

                metaDataWarmersService.putWarmer(putWarmerRequest, new ClusterStateUpdateListener<ClusterStateUpdateResponse>() {
                    @Override
                    public void onResponse(ClusterStateUpdateResponse response) {
                        listener.onResponse(new PutWarmerResponse(response.isAcknowledged()));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        listener.onFailure(t);
                    }
                });

            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }
}
