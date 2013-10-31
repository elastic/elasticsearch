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
import org.elasticsearch.action.support.master.TransportClusterStateUpdateAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.action.support.master.ClusterStateUpdateActionListener;
import org.elasticsearch.action.support.master.ClusterStateUpdateResponse;
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
public class TransportPutWarmerAction extends TransportClusterStateUpdateAction<PutWarmerClusterStateUpdateRequest, ClusterStateUpdateResponse, PutWarmerRequest, PutWarmerResponse> {

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
    protected PutWarmerResponse newResponse(ClusterStateUpdateResponse updateResponse) {
        return new PutWarmerResponse(updateResponse.isAcknowledged());
    }

    @Override
    protected PutWarmerClusterStateUpdateRequest newClusterStateUpdateRequest(PutWarmerRequest acknowledgedRequest) {
        PutWarmerClusterStateUpdateRequest updateRequest = new PutWarmerClusterStateUpdateRequest(acknowledgedRequest.name())
                .indices(acknowledgedRequest.searchRequest().indices())
                .types(acknowledgedRequest.searchRequest().types());

        if (acknowledgedRequest.searchRequest().source() != null && acknowledgedRequest.searchRequest().source().length() > 0) {
            updateRequest.source(acknowledgedRequest.searchRequest().source());
        } else if (acknowledgedRequest.searchRequest().extraSource() != null && acknowledgedRequest.searchRequest().extraSource().length() > 0) {
            updateRequest.source(acknowledgedRequest.searchRequest().extraSource());
        }

        return updateRequest;
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

                //if everything went well we update the cluster state as usual
                TransportPutWarmerAction.super.masterOperation(request, state, listener);
            }

            @Override
            public void onFailure(Throwable e) {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected void updateClusterState(PutWarmerClusterStateUpdateRequest updateRequest, ClusterStateUpdateActionListener<ClusterStateUpdateResponse, PutWarmerResponse> listener) {
        metaDataWarmersService.putWarmer(updateRequest, listener);
    }
}
