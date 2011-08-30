/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the get operation.
 *
 * @author kimchy (shay.banon)
 */
public class TransportGetAction extends TransportShardSingleOperationAction<GetRequest, GetResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final boolean realtime;

    @Inject public TransportGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                      IndicesService indicesService, ScriptService scriptService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.scriptService = scriptService;

        this.realtime = settings.getAsBoolean("action.get.realtime", true);
    }

    @Override protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override protected String transportAction() {
        return TransportActions.GET;
    }

    @Override protected String transportShardAction() {
        return "indices/get/shard";
    }

    @Override protected void checkBlock(GetRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, request.index());
    }

    @Override protected ShardIterator shards(ClusterState clusterState, GetRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.type(), request.id(), request.routing(), request.preference());
    }

    @Override protected void doExecute(GetRequest request, ActionListener<GetResponse> listener) {
        if (request.realtime == null) {
            request.realtime = this.realtime;
        }
        // update the routing (request#index here is possibly an alias)
        MetaData metaData = clusterService.state().metaData();
        request.routing(metaData.resolveIndexRouting(request.routing(), request.index()));

        super.doExecute(request, listener);
    }

    @Override protected GetResponse shardOperation(GetRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh(new Engine.Refresh(false));
        }

        GetResult result = indexShard.getService().get(request.type(), request.id(), request.fields(), request.realtime());
        return new GetResponse(result);
    }

    @Override protected GetRequest newRequest() {
        return new GetRequest();
    }

    @Override protected GetResponse newResponse() {
        return new GetResponse();
    }
}
