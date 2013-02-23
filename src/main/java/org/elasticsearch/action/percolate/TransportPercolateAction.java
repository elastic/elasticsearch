/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.percolate;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.support.single.custom.TransportSingleCustomOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.percolator.PercolatorExecutor;
import org.elasticsearch.index.percolator.PercolatorService;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 *
 */
public class TransportPercolateAction extends TransportSingleCustomOperationAction<PercolateRequest, PercolateResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportPercolateAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                    IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.PERCOLATE;
    }

    @Override
    protected PercolateRequest newRequest() {
        return new PercolateRequest();
    }

    @Override
    protected PercolateResponse newResponse() {
        return new PercolateResponse();
    }

    @Override
    protected String transportAction() {
        return PercolateAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PercolateRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PercolateRequest request) {
        request.index(state.metaData().concreteIndex(request.index()));
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, PercolateRequest request) {
        return clusterState.routingTable().index(request.index()).randomAllActiveShardsIt();
    }

    @Override
    protected PercolateResponse shardOperation(PercolateRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        PercolatorService percolatorService = indexService.percolateService();

        PercolatorExecutor.Response percolate = percolatorService.percolate(new PercolatorExecutor.SourceRequest(request.type(), request.source()));
        return new PercolateResponse(percolate.matches());
    }
}
