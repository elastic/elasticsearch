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

package org.elasticsearch.action.admin.indices.optimize;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Optimize index/indices action.
 */
public class TransportOptimizeAction extends TransportBroadcastOperationAction<OptimizeRequest, OptimizeResponse, ShardOptimizeRequest, ShardOptimizeResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportOptimizeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(settings, OptimizeAction.NAME, threadPool, clusterService, transportService, actionFilters,
                OptimizeRequest.class, ShardOptimizeRequest.class, ThreadPool.Names.OPTIMIZE);
        this.indicesService = indicesService;
    }

    @Override
    protected OptimizeResponse newResponse(OptimizeRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // a non active shard, ignore...
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                successfulShards++;
            }
        }
        return new OptimizeResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardOptimizeRequest newShardRequest(int numShards, ShardRouting shard, OptimizeRequest request) {
        return new ShardOptimizeRequest(shard.shardId(), request);
    }

    @Override
    protected ShardOptimizeResponse newShardResponse() {
        return new ShardOptimizeResponse();
    }

    @Override
    protected ShardOptimizeResponse shardOperation(ShardOptimizeRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).shardSafe(request.shardId().id());
        indexShard.optimize(request.optimizeRequest());
        return new ShardOptimizeResponse(request.shardId());
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, OptimizeRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, OptimizeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, OptimizeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
