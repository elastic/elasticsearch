/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.action.admin.indices.stats;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 */
public class TransportIndicesStatsAction extends TransportBroadcastOperationAction<IndicesStatsRequest, IndicesStatsResponse, TransportIndicesStatsAction.IndexShardStatsRequest, ShardStats> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesStatsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                       IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return IndicesStatsAction.NAME;
    }

    @Override
    protected IndicesStatsRequest newRequest() {
        return new IndicesStatsRequest();
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, IndicesStatsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allAssignedShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatsRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }


    @Override
    protected IndicesStatsResponse newResponse(IndicesStatsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        final List<ShardStats> shards = Lists.newArrayList();
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                shards.add((ShardStats) shardResponse);
                successfulShards++;
            }
        }
        return new IndicesStatsResponse(shards.toArray(new ShardStats[shards.size()]), clusterState, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndexShardStatsRequest newShardRequest() {
        return new IndexShardStatsRequest();
    }

    @Override
    protected IndexShardStatsRequest newShardRequest(ShardRouting shard, IndicesStatsRequest request) {
        return new IndexShardStatsRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardStats newShardResponse() {
        return new ShardStats();
    }

    @Override
    protected ShardStats shardOperation(IndexShardStatsRequest request) throws ElasticSearchException {
        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.index());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId());
        ShardStats stats = new ShardStats(indexShard.routingEntry());

        if (request.request.docs()) {
            stats.stats.docs = indexShard.docStats();
        }
        if (request.request.store()) {
            stats.stats.store = indexShard.storeStats();
        }
        if (request.request.indexing()) {
            stats.stats.indexing = indexShard.indexingStats(request.request.types());
        }
        if (request.request.get()) {
            stats.stats.get = indexShard.getStats();
        }
        if (request.request.search()) {
            stats.getStats().search = indexShard.searchStats(request.request.groups());
        }
        if (request.request.merge()) {
            stats.stats.merge = indexShard.mergeStats();
        }
        if (request.request.refresh()) {
            stats.stats.refresh = indexShard.refreshStats();
        }
        if (request.request.flush()) {
            stats.stats.flush = indexShard.flushStats();
        }
        if (request.request.warmer()) {
            stats.stats.warmer = indexShard.warmerStats();
        }
        if (request.request.filterCache()) {
            stats.stats.filterCache = indexShard.filterCacheStats();
        }
        if (request.request.idCache()) {
            stats.stats.idCache = indexShard.idCacheStats();
        }
        if (request.request.fieldData()) {
            stats.stats.fieldData = indexShard.fieldDataStats(request.request.fieldDataFields());
        }

        return stats;
    }

    public static class IndexShardStatsRequest extends BroadcastShardOperationRequest {

        // TODO if there are many indices, the request might hold a large indices array..., we don't really need to serialize it
        IndicesStatsRequest request;

        IndexShardStatsRequest() {
        }

        IndexShardStatsRequest(String index, int shardId, IndicesStatsRequest request) {
            super(index, shardId, request);
            this.request = request;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            request = new IndicesStatsRequest();
            request.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            request.writeTo(out);
        }
    }
}
