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

package org.elasticsearch.action.termvectors;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the get operation.
 */
public class TransportTermVectorsAction extends TransportShardSingleOperationAction<TermVectorsRequest, TermVectorsResponse> {

    private final IndicesService indicesService;

    @Override
    protected void doExecute(TermVectorsRequest request, ActionListener<TermVectorsResponse> listener) {
        request.startTime = System.currentTimeMillis();
        super.doExecute(request, listener);
    }

    @Inject
    public TransportTermVectorsAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, TermVectorsAction.NAME, threadPool, clusterService, transportService, actionFilters,
                TermVectorsRequest.class, ThreadPool.Names.GET);
        this.indicesService = indicesService;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting().getShards(state, request.concreteIndex(), request.request().type(), request.request().id(),
                request.request().routing(), request.request().preference());
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias)
        request.request().routing(state.metaData().resolveIndexRouting(request.request().routing(), request.request().index()));
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetaData().routingRequired(request.concreteIndex(), request.request().type())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
    }

    @Override
    protected TermVectorsResponse shardOperation(TermVectorsRequest request, ShardId shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());
        TermVectorsResponse response = indexShard.termVectorsService().getTermVectors(request, shardId.getIndex());
        response.updateTookInMillis(request.startTime());
        return response;
    }

    @Override
    protected TermVectorsResponse newResponse() {
        return new TermVectorsResponse();
    }
}
