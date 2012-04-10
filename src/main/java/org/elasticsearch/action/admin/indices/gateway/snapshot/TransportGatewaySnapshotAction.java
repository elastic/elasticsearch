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

package org.elasticsearch.action.admin.indices.gateway.snapshot;

import com.google.common.collect.Lists;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.action.ShardOperationFailedException;
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
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportGatewaySnapshotAction extends TransportBroadcastOperationAction<GatewaySnapshotRequest, GatewaySnapshotResponse, ShardGatewaySnapshotRequest, ShardGatewaySnapshotResponse> {

    private final IndicesService indicesService;

    @Inject
    public TransportGatewaySnapshotAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                          TransportService transportService, IndicesService indicesService) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SNAPSHOT;
    }

    @Override
    protected String transportAction() {
        return GatewaySnapshotAction.NAME;
    }

    @Override
    protected GatewaySnapshotRequest newRequest() {
        return new GatewaySnapshotRequest();
    }

    @Override
    protected boolean ignoreNonActiveExceptions() {
        return true;
    }

    @Override
    protected GatewaySnapshotResponse newResponse(GatewaySnapshotRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
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
                    shardFailures = Lists.newArrayList();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                successfulShards++;
            }
        }
        return new GatewaySnapshotResponse(shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected ShardGatewaySnapshotRequest newShardRequest() {
        return new ShardGatewaySnapshotRequest();
    }

    @Override
    protected ShardGatewaySnapshotRequest newShardRequest(ShardRouting shard, GatewaySnapshotRequest request) {
        return new ShardGatewaySnapshotRequest(shard.index(), shard.id());
    }

    @Override
    protected ShardGatewaySnapshotResponse newShardResponse() {
        return new ShardGatewaySnapshotResponse();
    }

    @Override
    protected ShardGatewaySnapshotResponse shardOperation(ShardGatewaySnapshotRequest request) throws ElasticSearchException {
        IndexShardGatewayService shardGatewayService = indicesService.indexServiceSafe(request.index())
                .shardInjectorSafe(request.shardId()).getInstance(IndexShardGatewayService.class);
        shardGatewayService.snapshot("api");
        return new ShardGatewaySnapshotResponse(request.index(), request.shardId());
    }

    /**
     * The snapshot request works against all primary shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState clusterState, GatewaySnapshotRequest request, String[] concreteIndices) {
        return clusterState.routingTable().activePrimaryShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, GatewaySnapshotRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, GatewaySnapshotRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

}
