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

package org.elasticsearch.action.admin.indices.exists.indices;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Indices exists action.
 */
public class TransportIndicesExistsAction extends TransportMasterNodeOperationAction<IndicesExistsRequest, IndicesExistsResponse> {

    @Inject
    public TransportIndicesExistsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                        ThreadPool threadPool) {
        super(settings, transportService, clusterService, threadPool);
    }

    @Override
    protected String executor() {
        // lightweight in memory check
        return ThreadPool.Names.SAME;
    }

    @Override
    protected String transportAction() {
        return IndicesExistsAction.NAME;
    }

    @Override
    protected IndicesExistsRequest newRequest() {
        return new IndicesExistsRequest();
    }

    @Override
    protected IndicesExistsResponse newResponse() {
        return new IndicesExistsResponse();
    }

    @Override
    protected void doExecute(IndicesExistsRequest request, ActionListener<IndicesExistsResponse> listener) {
        // don't call this since it will throw IndexMissingException
        //request.indices(clusterService.state().metaData().concreteIndices(request.indices()));
        super.doExecute(request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesExistsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, request.indices());
    }

    @Override
    protected void masterOperation(final IndicesExistsRequest request, final ClusterState state, final ActionListener<IndicesExistsResponse> listener) throws ElasticSearchException {
        boolean exists = true;
        for (String index : request.indices()) {
            if (!state.metaData().hasConcreteIndex(index)) {
                exists = false;
            }
        }
        listener.onResponse(new IndicesExistsResponse(exists));
    }
}
