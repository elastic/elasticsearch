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

package org.elasticsearch.legacy.action.admin.indices.refresh;

import org.elasticsearch.legacy.ElasticsearchException;
import org.elasticsearch.legacy.action.ShardOperationFailedException;
import org.elasticsearch.legacy.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.legacy.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.legacy.cluster.ClusterService;
import org.elasticsearch.legacy.cluster.ClusterState;
import org.elasticsearch.legacy.cluster.block.ClusterBlockException;
import org.elasticsearch.legacy.cluster.block.ClusterBlockLevel;
import org.elasticsearch.legacy.cluster.routing.GroupShardsIterator;
import org.elasticsearch.legacy.cluster.routing.ShardRouting;
import org.elasticsearch.legacy.common.inject.Inject;
import org.elasticsearch.legacy.common.settings.Settings;
import org.elasticsearch.legacy.index.engine.Engine;
import org.elasticsearch.legacy.index.shard.service.IndexShard;
import org.elasticsearch.legacy.indices.IndicesService;
import org.elasticsearch.legacy.threadpool.ThreadPool;
import org.elasticsearch.legacy.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * Refresh action.
 */
public class TransportRefreshAction extends TransportBroadcastOperationAction<RefreshRequest, RefreshResponse, ShardRefreshRequest, ShardRefreshResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportRefreshAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                  TransportService transportService, IndicesService indicesService) {
        super(settings, RefreshAction.NAME, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.REFRESH;
    }

    @Override
    protected RefreshRequest newRequest() {
        return new RefreshRequest();
    }

    @Override
    protected RefreshResponse newResponse(RefreshRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // non active shard, ignore
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
        return new RefreshResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardRefreshRequest newShardRequest() {
        return new ShardRefreshRequest();
    }

    @Override
    protected ShardRefreshRequest newShardRequest(int numShards, ShardRouting shard, RefreshRequest request) {
        return new ShardRefreshRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardRefreshResponse newShardResponse() {
        return new ShardRefreshResponse();
    }

    @Override
    protected ShardRefreshResponse shardOperation(ShardRefreshRequest request) throws ElasticsearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());
        indexShard.refresh(new Engine.Refresh("api").force(request.force()));
        logger.trace("{} refresh request executed, force: [{}]", indexShard.shardId(), request.force());
        return new ShardRefreshResponse(request.index(), request.shardId());
    }

    /**
     * The refresh request works against *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, RefreshRequest request, String[] concreteIndices) {
        return clusterState.routingTable().allAssignedShardsGrouped(concreteIndices, true, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RefreshRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RefreshRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }
}
