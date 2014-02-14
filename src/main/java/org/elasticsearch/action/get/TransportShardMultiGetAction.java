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

package org.elasticsearch.action.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.fetch.source.FetchSourceContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportShardMultiGetAction extends TransportShardSingleOperationAction<MultiGetShardRequest, MultiGetShardResponse> {

    private final IndicesService indicesService;

    private final boolean realtime;

    @Inject
    public TransportShardMultiGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, ThreadPool threadPool) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;

        this.realtime = settings.getAsBoolean("action.get.realtime", true);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.GET;
    }

    @Override
    protected String transportAction() {
        return MultiGetAction.NAME + "/shard";
    }

    @Override
    protected MultiGetShardRequest newRequest() {
        return new MultiGetShardRequest();
    }

    @Override
    protected MultiGetShardResponse newResponse() {
        return new MultiGetShardResponse();
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, MultiGetShardRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, MultiGetShardRequest request) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, request.index());
    }

    @Override
    protected ShardIterator shards(ClusterState state, MultiGetShardRequest request) {
        return clusterService.operationRouting()
                .getShards(clusterService.state(), request.index(), request.shardId(), request.preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, MultiGetShardRequest request) {
        if (request.realtime == null) {
            request.realtime = this.realtime;
        }
        // no need to set concrete index and routing here, it has already been set by the multi get action on the item
        //request.index(state.metaData().concreteIndex(request.index()));
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, int shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(request.index());
        IndexShard indexShard = indexService.shardSafe(shardId);

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh(new Engine.Refresh("refresh_flag_mget").force(TransportGetAction.REFRESH_FORCE));
        }

        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            String type = request.types.get(i);
            String id = request.ids.get(i);
            String[] fields = request.fields.get(i);

            long version = request.versions.get(i);
            VersionType versionType = request.versionTypes.get(i);
            if (versionType == null) {
                versionType = VersionType.INTERNAL;
            }

            FetchSourceContext fetchSourceContext = request.fetchSourceContexts.get(i);
            try {
                GetResult getResult = indexShard.getService().get(type, id, fields, request.realtime(), version, versionType, fetchSourceContext);
                response.add(request.locations.get(i), new GetResponse(getResult));
            } catch (Throwable t) {
                if (TransportActions.isShardNotAvailableException(t)) {
                    throw (ElasticsearchException) t;
                } else {
                    logger.debug("[{}][{}] failed to execute multi_get for [{}]/[{}]", t, request.index(), shardId, type, id);
                    response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), type, id, ExceptionsHelper.detailedMessage(t)));
                }
            }
        }

        return response;
    }

}
