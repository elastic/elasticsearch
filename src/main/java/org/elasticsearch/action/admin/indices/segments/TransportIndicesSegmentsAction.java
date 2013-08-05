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

package org.elasticsearch.action.admin.indices.segments;

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
 *
 */
public class TransportIndicesSegmentsAction extends TransportBroadcastOperationAction<IndicesSegmentsRequest, IndicesSegmentResponse, TransportIndicesSegmentsAction.IndexShardSegmentRequest, ShardSegments> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesSegmentsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
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
        return IndicesSegmentsAction.NAME;
    }

    @Override
    protected IndicesSegmentsRequest newRequest() {
        return new IndicesSegmentsRequest();
    }

    /**
     * Segments goes across *all* active shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, IndicesSegmentsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    @Override
    protected IndicesSegmentResponse newResponse(IndicesSegmentsRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        final List<ShardSegments> shards = newArrayList();
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
                shards.add((ShardSegments) shardResponse);
                successfulShards++;
            }
        }
        return new IndicesSegmentResponse(shards.toArray(new ShardSegments[shards.size()]), clusterState, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndexShardSegmentRequest newShardRequest() {
        return new IndexShardSegmentRequest();
    }

    @Override
    protected IndexShardSegmentRequest newShardRequest(ShardRouting shard, IndicesSegmentsRequest request) {
        return new IndexShardSegmentRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardSegments newShardResponse() {
        return new ShardSegments();
    }

    @Override
    protected ShardSegments shardOperation(IndexShardSegmentRequest request) throws ElasticSearchException {
        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.index());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId());
        return new ShardSegments(indexShard.routingEntry(), indexShard.engine().segments());
    }

    public static class IndexShardSegmentRequest extends BroadcastShardOperationRequest {

        IndexShardSegmentRequest() {
        }

        IndexShardSegmentRequest(String index, int shardId, IndicesSegmentsRequest request) {
            super(index, shardId, request);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }
    }
}
