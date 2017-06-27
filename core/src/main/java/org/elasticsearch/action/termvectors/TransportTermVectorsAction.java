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

import org.elasticsearch.action.RoutingMissingException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.single.shard.TransportSingleShardAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.termvectors.TermVectorsService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the get operation.
 */
public class TransportTermVectorsAction extends TransportSingleShardAction<TermVectorsRequest, TermVectorsResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportTermVectorsAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, TermVectorsAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                TermVectorsRequest::new, ThreadPool.Names.GET);
        this.indicesService = indicesService;

    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        if (request.request().doc() != null && request.request().routing() == null) {
            // artificial document without routing specified, ignore its "id" and use either random shard or according to preference
            GroupShardsIterator<ShardIterator> groupShardsIter = clusterService.operationRouting().searchShards(state,
                    new String[] { request.concreteIndex() }, null, request.request().preference());
            return groupShardsIter.iterator().next();
        }

        return clusterService.operationRouting().getShards(state, request.concreteIndex(), request.request().id(),
                request.request().routing(), request.request().preference());
    }

    @Override
    protected boolean resolveIndex(TermVectorsRequest request) {
        return true;
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        // update the routing (request#index here is possibly an alias or a parent)
        request.request().routing(state.metaData().resolveIndexRouting(request.request().parent(), request.request().routing(), request.request().index()));
        // Fail fast on the node that received the request.
        if (request.request().routing() == null && state.getMetaData().routingRequired(request.concreteIndex(), request.request().type())) {
            throw new RoutingMissingException(request.concreteIndex(), request.request().type(), request.request().id());
        }
    }

    @Override
    protected TermVectorsResponse shardOperation(TermVectorsRequest request, ShardId shardId) {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.getShard(shardId.id());
        return TermVectorsService.getTermVectors(indexShard, request);
    }

    @Override
    protected TermVectorsResponse newResponse() {
        return new TermVectorsResponse();
    }
}
