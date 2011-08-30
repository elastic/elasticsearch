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
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
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

public class TransportShardMultiGetAction extends TransportShardSingleOperationAction<MultiGetShardRequest, MultiGetShardResponse> {

    private final IndicesService indicesService;

    private final ScriptService scriptService;

    private final boolean realtime;

    @Inject public TransportShardMultiGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
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
        return "indices/mget/shard";
    }

    @Override protected String transportShardAction() {
        return "indices/mget/shard/s";
    }

    @Override protected MultiGetShardRequest newRequest() {
        return new MultiGetShardRequest();
    }

    @Override protected MultiGetShardResponse newResponse() {
        return new MultiGetShardResponse();
    }

    @Override protected void checkBlock(MultiGetShardRequest request, ClusterState state) {
        state.blocks().indexBlockedRaiseException(ClusterBlockLevel.READ, request.index());
    }

    @Override protected ShardIterator shards(ClusterState clusterState, MultiGetShardRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.shardId(), request.preference());
    }

    @Override protected void doExecute(MultiGetShardRequest request, ActionListener<MultiGetShardResponse> listener) {
        if (request.realtime == null) {
            request.realtime = this.realtime;
        }
        super.doExecute(request, listener);
    }

    @Override protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, int shardId) throws ElasticSearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh(new Engine.Refresh(false));
        }

        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            String type = request.types.get(i);
            String id = request.ids.get(i);
            String[] fields = request.fields.get(i);

            try {
                GetResult getResult = indexShard.getService().get(type, id, fields, request.realtime());
                response.add(request.locations.get(i), new GetResponse(getResult));
            } catch (Exception e) {
                logger.debug("[{}][{}] failed to execute multi_get for [{}]/[{}]", e, request.index(), shardId, type, id);
                response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), type, id, ExceptionsHelper.detailedMessage(e)));
            }
        }

        return response;
    }

}