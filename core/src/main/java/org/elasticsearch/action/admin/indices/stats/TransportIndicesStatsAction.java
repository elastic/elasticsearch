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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardsIterator;
import org.elasticsearch.cluster.service.ClusterService;
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
import java.util.List;

/**
 */
public class TransportIndicesStatsAction extends TransportBroadcastByNodeAction<IndicesStatsRequest, IndicesStatsResponse, ShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                       TransportService transportService, IndicesService indicesService,
                                       ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IndicesStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                IndicesStatsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardStats readShardResult(StreamInput in) throws IOException {
        return ShardStats.readShardStats(in);
    }

    @Override
    protected IndicesStatsResponse newResponse(IndicesStatsRequest request, int totalShards, int successfulShards, int failedShards, List<ShardStats> responses, List<ShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new IndicesStatsResponse(responses.toArray(new ShardStats[responses.size()]), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndicesStatsRequest readRequestFrom(StreamInput in) throws IOException {
        IndicesStatsRequest request = new IndicesStatsRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected ShardStats shardOperation(IndicesStatsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(shardRouting.shardId().id());
        // if we don't have the routing entry yet, we need it stats wise, we treat it as if the shard is not ready yet
        if (indexShard.routingEntry() == null) {
            throw new ShardNotFoundException(indexShard.shardId());
        }

        CommonStatsFlags flags = new CommonStatsFlags().clear();

        if (request.docs()) {
            flags.set(CommonStatsFlags.Flag.Docs);
        }
        if (request.store()) {
            flags.set(CommonStatsFlags.Flag.Store);
        }
        if (request.indexing()) {
            flags.set(CommonStatsFlags.Flag.Indexing);
            flags.types(request.types());
        }
        if (request.get()) {
            flags.set(CommonStatsFlags.Flag.Get);
        }
        if (request.search()) {
            flags.set(CommonStatsFlags.Flag.Search);
            flags.groups(request.groups());
        }
        if (request.merge()) {
            flags.set(CommonStatsFlags.Flag.Merge);
        }
        if (request.refresh()) {
            flags.set(CommonStatsFlags.Flag.Refresh);
        }
        if (request.flush()) {
            flags.set(CommonStatsFlags.Flag.Flush);
        }
        if (request.warmer()) {
            flags.set(CommonStatsFlags.Flag.Warmer);
        }
        if (request.queryCache()) {
            flags.set(CommonStatsFlags.Flag.QueryCache);
        }
        if (request.fieldData()) {
            flags.set(CommonStatsFlags.Flag.FieldData);
            flags.fieldDataFields(request.fieldDataFields());
        }
        if (request.segments()) {
            flags.set(CommonStatsFlags.Flag.Segments);
            flags.includeSegmentFileSizes(request.includeSegmentFileSizes());
        }
        if (request.completion()) {
            flags.set(CommonStatsFlags.Flag.Completion);
            flags.completionDataFields(request.completionFields());
        }
        if (request.translog()) {
            flags.set(CommonStatsFlags.Flag.Translog);
        }
        if (request.suggest()) {
            flags.set(CommonStatsFlags.Flag.Suggest);
        }
        if (request.requestCache()) {
            flags.set(CommonStatsFlags.Flag.RequestCache);
        }
        if (request.recovery()) {
            flags.set(CommonStatsFlags.Flag.Recovery);
        }

        return new ShardStats(indexShard.routingEntry(), indexShard.shardPath(), new CommonStats(indicesService.getIndicesQueryCache(), indexShard, flags), indexShard.commitStats());
    }
}
