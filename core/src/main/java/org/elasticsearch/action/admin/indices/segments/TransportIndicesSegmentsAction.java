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

package org.elasticsearch.action.admin.indices.segments;

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
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

public class TransportIndicesSegmentsAction extends TransportBroadcastByNodeAction<IndicesSegmentsRequest, IndicesSegmentResponse, ShardSegments> {

    private final IndicesService indicesService;

    @Inject
    public TransportIndicesSegmentsAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                          IndicesService indicesService, ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, IndicesSegmentsAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                IndicesSegmentsRequest::new, ThreadPool.Names.MANAGEMENT);
        this.indicesService = indicesService;
    }

    /**
     * Segments goes across *all* active shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, IndicesSegmentsRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesSegmentsRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesSegmentsRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, concreteIndices);
    }

    @Override
    protected ShardSegments readShardResult(StreamInput in) throws IOException {
        return ShardSegments.readShardSegments(in);
    }

    @Override
    protected IndicesSegmentResponse newResponse(IndicesSegmentsRequest request, int totalShards, int successfulShards, int failedShards, List<ShardSegments> results, List<ShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new IndicesSegmentResponse(results.toArray(new ShardSegments[results.size()]), totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndicesSegmentsRequest readRequestFrom(StreamInput in) throws IOException {
        final IndicesSegmentsRequest request = new IndicesSegmentsRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected ShardSegments shardOperation(IndicesSegmentsRequest request, ShardRouting shardRouting) {
        IndexService indexService = indicesService.indexServiceSafe(shardRouting.index());
        IndexShard indexShard = indexService.getShard(shardRouting.id());
        return new ShardSegments(indexShard.routingEntry(), indexShard.segments(request.verbose()));
    }
}
