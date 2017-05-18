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

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.broadcast.node.TransportBroadcastByNodeAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.PlainShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs collection of {@link PhantomShardStats} on nodes with phantom shards.
 */
public class TransportPhantomIndicesStatsAction extends TransportBroadcastByNodeAction<PhantomIndicesStatsRequest, PhantomIndicesStatsResponse, PhantomShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportPhantomIndicesStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                              TransportService transportService, ActionFilters actionFilters,
                                              IndexNameExpressionResolver indexNameExpressionResolver,
                                              IndicesService indicesService) {
        super(settings, PhantomIndicesStatsAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, PhantomIndicesStatsRequest.class, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    @Override
    protected PhantomShardStats readShardResult(StreamInput in) throws IOException {
        return PhantomShardStats.read(in);
    }

    @Override
    protected PhantomIndicesStatsResponse newResponse(PhantomIndicesStatsRequest request, int totalShards, int successfulShards, int failedShards, List<PhantomShardStats> phantomShardStats, List<ShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new PhantomIndicesStatsResponse(
            phantomShardStats.toArray(new PhantomShardStats[phantomShardStats.size()]),
            totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected PhantomIndicesStatsRequest readRequestFrom(StreamInput in) throws IOException {
        PhantomIndicesStatsRequest request = new PhantomIndicesStatsRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected PhantomShardStats shardOperation(PhantomIndicesStatsRequest request, ShardRouting shardRouting) throws IOException {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(shardRouting.shardId().id());
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }
        if (indexShard.usesPhantomEngine()) {
            return new PhantomShardStats(indexShard);
        }
        return null;
    }

    @Override
    protected ShardsIterator shards(ClusterState clusterState, PhantomIndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, PhantomIndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, PhantomIndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }
}
