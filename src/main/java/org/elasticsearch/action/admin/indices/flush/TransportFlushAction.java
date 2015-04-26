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

package org.elasticsearch.action.admin.indices.flush;

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
 * Flush Action.
 */
public class TransportFlushAction extends TransportBroadcastOperationAction<FlushRequest, FlushResponse, ShardFlushRequest, ShardFlushResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportFlushAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService, ActionFilters actionFilters) {
        super(settings, FlushAction.NAME, threadPool, clusterService, transportService, actionFilters,
                FlushRequest.class, ShardFlushRequest.class, ThreadPool.Names.FLUSH);
        this.indicesService = indicesService;
    }

    @Override
    protected FlushResponse newResponse(FlushRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // a non active shard, ignore
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
        return new FlushResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardFlushRequest newShardRequest(int numShards, ShardRouting shard, FlushRequest request) {
        return new ShardFlushRequest(shard.shardId(), request);
    }

    @Override
    protected ShardFlushResponse newShardResponse() {
        return new ShardFlushResponse();
    }

    @Override
    protected ShardFlushResponse shardOperation(ShardFlushRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).shardSafe(request.shardId().id());
        indexShard.flush(request.getRequest());
        return new ShardFlushResponse(request.shardId());
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, FlushRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allActiveShardsGrouped(concreteIndices, true, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, FlushRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, FlushRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_WRITE, concreteIndices);
    }
}
