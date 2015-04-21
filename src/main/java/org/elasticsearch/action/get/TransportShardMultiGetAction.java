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
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.single.shard.TransportShardSingleOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

public class TransportShardMultiGetAction extends TransportShardSingleOperationAction<MultiGetShardRequest, MultiGetShardResponse> {

    private static final String ACTION_NAME = MultiGetAction.NAME + "[shard]";

    private final IndicesService indicesService;

    private final boolean realtime;

    @Inject
    public TransportShardMultiGetAction(Settings settings, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, ThreadPool threadPool, ActionFilters actionFilters) {
        super(settings, ACTION_NAME, threadPool, clusterService, transportService, actionFilters,
                MultiGetShardRequest.class, ThreadPool.Names.GET);
        this.indicesService = indicesService;

        this.realtime = settings.getAsBoolean("action.get.realtime", true);
    }

    @Override
    protected boolean isSubAction() {
        return true;
    }

    @Override
    protected MultiGetShardResponse newResponse() {
        return new MultiGetShardResponse();
    }

    @Override
    protected boolean resolveIndex() {
        return true;
    }

    @Override
    protected ShardIterator shards(ClusterState state, InternalRequest request) {
        return clusterService.operationRouting()
                .getShards(state, request.request().index(), request.request().shardId(), request.request().preference());
    }

    @Override
    protected void resolveRequest(ClusterState state, InternalRequest request) {
        if (request.request().realtime == null) {
            request.request().realtime = this.realtime;
        }
    }

    @Override
    protected MultiGetShardResponse shardOperation(MultiGetShardRequest request, ShardId shardId) throws ElasticsearchException {
        IndexService indexService = indicesService.indexServiceSafe(shardId.getIndex());
        IndexShard indexShard = indexService.shardSafe(shardId.id());

        if (request.refresh() && !request.realtime()) {
            indexShard.refresh("refresh_flag_mget");
        }

        MultiGetShardResponse response = new MultiGetShardResponse();
        for (int i = 0; i < request.locations.size(); i++) {
            MultiGetRequest.Item item = request.items.get(i);
            try {
                GetResult getResult = indexShard.getService().get(item.type(), item.id(), item.fields(), request.realtime(), item.version(), item.versionType(), item.fetchSourceContext(), request.ignoreErrorsOnGeneratedFields());
                response.add(request.locations.get(i), new GetResponse(getResult));
            } catch (Throwable t) {
                if (TransportActions.isShardNotAvailableException(t)) {
                    throw (ElasticsearchException) t;
                } else {
                    logger.debug("{} failed to execute multi_get for [{}]/[{}]", t, shardId, item.type(), item.id());
                    response.add(request.locations.get(i), new MultiGetResponse.Failure(request.index(), item.type(), item.id(), ExceptionsHelper.detailedMessage(t)));
                }
            }
        }

        return response;
    }
}
