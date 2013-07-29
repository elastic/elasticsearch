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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaDataIndexStateService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Delete index action.
 */
public class TransportCloseIndexAction extends TransportMasterNodeOperationAction<CloseIndexRequest, CloseIndexResponse> {

    private final MetaDataIndexStateService indexStateService;
    private final boolean disableCloseAllIndices;


    @Inject
    public TransportCloseIndexAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                     ThreadPool threadPool, MetaDataIndexStateService indexStateService) {
        super(settings, transportService, clusterService, threadPool);
        this.indexStateService = indexStateService;
        this.disableCloseAllIndices = settings.getAsBoolean("action.disable_close_all_indices", false);
    }

    @Override
    protected String executor() {
        // no need to use a thread pool, we go async right away
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return CloseIndexAction.NAME;
    }

    @Override
    protected CloseIndexRequest newRequest() {
        return new CloseIndexRequest();
    }

    @Override
    protected CloseIndexResponse newResponse() {
        return new CloseIndexResponse();
    }

    @Override
    protected void doExecute(CloseIndexRequest request, ActionListener<CloseIndexResponse> listener) {
        ClusterState state = clusterService.state();
        String[] indicesOrAliases = request.indices();
        request.indices(state.metaData().concreteIndices(indicesOrAliases, request.ignoreIndices(), false));

        if (disableCloseAllIndices) {
            if (state.metaData().isExplicitAllIndices(indicesOrAliases) ||
                    state.metaData().isPatternMatchingAllIndices(indicesOrAliases, request.indices())) {
                throw new ElasticSearchIllegalArgumentException("closing all indices is disabled");
            }
        }

        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(CloseIndexRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected void masterOperation(final CloseIndexRequest request, final ClusterState state, final ActionListener<CloseIndexResponse> listener) throws ElasticSearchException {
        indexStateService.closeIndex(new MetaDataIndexStateService.Request(request.indices()).timeout(request.timeout()).masterTimeout(request.masterNodeTimeout()), new MetaDataIndexStateService.Listener() {
            @Override
            public void onResponse(MetaDataIndexStateService.Response response) {
                listener.onResponse(new CloseIndexResponse(response.acknowledged()));
            }

            @Override
            public void onFailure(Throwable t) {
                logger.debug("failed to close indices [{}]", t, request.indices());
                listener.onFailure(t);
            }
        });
    }
}
