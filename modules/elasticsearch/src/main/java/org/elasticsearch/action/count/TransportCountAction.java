/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.action.count;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.TransportBroadcastOperationAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportCountAction extends TransportBroadcastOperationAction<CountRequest, CountResponse, ShardCountRequest, ShardCountResponse> {

    private final IndicesService indicesService;

    @Inject public TransportCountAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override protected String executor() {
        return ThreadPool.Names.SEARCH;
    }

    @Override protected String transportAction() {
        return TransportActions.COUNT;
    }

    @Override protected String transportShardAction() {
        return "indices/count/shard";
    }

    @Override protected CountRequest newRequest() {
        return new CountRequest();
    }

    @Override protected ShardCountRequest newShardRequest() {
        return new ShardCountRequest();
    }

    @Override protected ShardCountRequest newShardRequest(ShardRouting shard, CountRequest request) {
        return new ShardCountRequest(shard.index(), shard.id(), request);
    }

    @Override protected ShardCountResponse newShardResponse() {
        return new ShardCountResponse();
    }

    @Override protected GroupShardsIterator shards(CountRequest request, ClusterState clusterState) {
        return clusterService.operationRouting().searchShards(clusterState, request.indices(), request.queryHint(), request.routing(), null);
    }

    @Override protected void checkBlock(CountRequest request, ClusterState state) {
        for (String index : request.indices()) {
            state.blocks().indexBlocked(ClusterBlockLevel.READ, index);
        }
    }

    @Override protected CountResponse newResponse(CountRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        long count = 0;
        List<ShardOperationFailedException> shardFailures = null;
        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                failedShards++;
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                count += ((ShardCountResponse) shardResponse).count();
                successfulShards++;
            }
        }
        return new CountResponse(count, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override protected ShardCountResponse shardOperation(ShardCountRequest request) throws ElasticSearchException {
        IndexShard indexShard = indicesService.indexServiceSafe(request.index()).shardSafe(request.shardId());
        long count = indexShard.count(request.minScore(), request.querySource(), request.querySourceOffset(), request.querySourceLength(),
                request.queryParserName(), request.types());
        return new ShardCountResponse(request.index(), request.shardId(), count);
    }
}
