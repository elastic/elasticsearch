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

package org.elasticsearch.legacy.action.get;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.RoutingMissingException;
import org.elasticsearch.legacy.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.ShardIterator;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.engine.Engine;
import org.elasticsearch.legacy.index.get.GetResult;
import org.elasticsearch.legacy.index.service.IndexService;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

/**
 * Performs the get operation.
 */
public class TransportGetAction extends TransportShardSingleOperationAction<GetRequest, GetResponse> {

    public static final boolean REFRESH_FORCE = false;

    private final IndicesService indicesService;
    private final boolean realtime;

    @Inject
    public TransportGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                              IndicesService indicesService, ThreadPool threadPool) {
        super(settings, GetAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;

        this.realtime = settings.getAsBoolean("action.get.realtime", true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, GetRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, GetRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, GetRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing(), request.preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, GetRequest request) {
        if (request.realtime == null) {
            request.realtime = this.realtime;
        }
        // update the routing (request#index here is possibly an alias)
        request.routing(state.metaData().resolveIndexRouting(request.routing(), request.index()));
        request.index(state.metaData().concreteSingleIndex(request.index()));

        // Fail fast on the node that received the request.
        if (request.routing() == null && state.getMetaData().routingRequired(request.index(), request.type())) {
            throw new RoutingMissingException(request.index(), request.type(), request.id());
        }
    }

    @Override
    protected GetResponse shardOperation(GetRequest request, int shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh(new Engine.Refresh("refresh_flag_get").force(REFRESH_FORCE));
        }

        GetResult result = indexShard.getService().get(request.type(), request.id(), request.fields(),
                request.realtime(), request.version(), request.versionType(), request.fetchSourceContext());
        return new GetResponse(result);
    }

    @Override
    protected GetRequest newRequest() {
        return new GetRequest();
    }

    @Override
    protected GetResponse newResponse() {
        return new GetResponse();
    }
}
