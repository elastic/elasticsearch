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

package org.elasticsearch.action.termvector;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the get operation.
 */
public class TransportSingleShardTermVectorAction extends TransportShardSingleOperationAction<TermVectorRequest, TermVectorResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportSingleShardTermVectorAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                                IndicesService indicesService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        // TODO: Is this the right pool to execute this on?
        return ThreadPool.Names.GET;
    }

    @Override
    protected String transportAction() {
        return TermVectorAction.NAME;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, TermVectorRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, TermVectorRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, TermVectorRequest request) {
        return clusterService.operationRouting().getShards(clusterService.state(), request.index(), request.type(), request.id(),
                request.routing(), request.preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, TermVectorRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.routing(state.metaData().resolveIndexRouting(request.routing(), request.index()));
        request.index(state.metaData().concreteSingleIndex(request.index()));

        // Fail fast on the node that received the request.
        if (request.routing() == null && state.getMetaData().routingRequired(request.index(), request.type())) {
            throw new RoutingMissingException(request.index(), request.type(), request.id());
        }
    }

    @Override
    protected TermVectorResponse shardOperation(TermVectorRequest request, int shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);
        return indexShard.termVectorService().getTermVector(request);
    }

    @Override
    protected TermVectorRequest newRequest() {
        return new TermVectorRequest();
    }

    @Override
    protected TermVectorResponse newResponse() {
        return new TermVectorResponse();
    }

}
