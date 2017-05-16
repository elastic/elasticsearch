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

package org.elasticsearch.legacy.action.admin.indices.segments;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ShardOperationFailedException;
import org.elasticsearch.legacy.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationRequest;
import org.elasticsearch.legacy.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.GroupShardsIterator;
import org.elasticsearch.legacy.cluster.routing.ShardRouting;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.io.stream.StreamInput;
import org.elasticsearch.legacy.common.io.stream.StreamOutput;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.service.InternalIndexService;
import org.elasticsearch.legacy.index.shard.service.InternalIndexShard;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

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
        super(settings, IndicesSegmentsAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
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
    protected IndexShardSegmentRequest newShardRequest(int numShards, ShardRouting shard, IndicesSegmentsRequest request) {
        return new IndexShardSegmentRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardSegments newShardResponse() {
        return new ShardSegments();
    }

    @Override
    protected ShardSegments shardOperation(IndexShardSegmentRequest request) throws ElasticsearchException {
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
