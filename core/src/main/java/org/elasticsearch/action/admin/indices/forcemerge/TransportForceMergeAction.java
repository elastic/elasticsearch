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

package org.elasticsearch.action.admin.indices.forcemerge;

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
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;

/**
 * ForceMerge index/indices action.
 */
public class TransportForceMergeAction extends TransportBroadcastByNodeAction<ForceMergeRequest, ForceMergeResponse, TransportBroadcastByNodeAction.EmptyResult> {

    private final IndicesService indicesService;

    @Inject
    public TransportForceMergeAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService, IndicesService indicesService,
                                   ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ForceMergeAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                ForceMergeRequest::new, ThreadPool.Names.FORCE_MERGE);
        this.indicesService = indicesService;
    }

    @Override
    protected EmptyResult readShardResult(StreamInput in) throws IOException {
        return EmptyResult.readEmptyResultFrom(in);
    }

    @Override
    protected ForceMergeResponse newResponse(ForceMergeRequest request, int totalShards, int successfulShards, int failedShards, List<EmptyResult> responses, List<ShardOperationFailedException> shardFailures, ClusterState clusterState) {
        return new ForceMergeResponse(totalShards, successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ForceMergeRequest readRequestFrom(StreamInput in) throws IOException {
        final ForceMergeRequest request = new ForceMergeRequest();
        request.readFrom(in);
        return request;
    }

    @Override
    protected EmptyResult shardOperation(ForceMergeRequest request, ShardRouting shardRouting) throws IOException {
        IndexShard indexShard = indicesService.indexServiceSafe(shardRouting.shardId().getIndex()).getShard(shardRouting.shardId().id());
        indexShard.forceMerge(request);
        return EmptyResult.INSTANCE;
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected ShardsIterator shards(ClusterState clusterState, ForceMergeRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allShards(concreteIndices);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, ForceMergeRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, ForceMergeRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
