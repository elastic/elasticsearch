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


package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryMetrics;
import org.elasticsearch.index.shard.IndexShardState;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 *
 */
public class TransportRecoveryAction extends
        TransportBroadcastOperationAction<RecoveryRequest, RecoveryResponse, TransportRecoveryAction.ShardRecoveryRequest, ShardRecoveryResponse> {

    private final IndicesService indicesService;
    private final RecoveryTarget recoveryTarget;

    @Inject
    public TransportRecoveryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                   TransportService transportService, IndicesService indicesService, RecoveryTarget recoveryTarget) {

        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.recoveryTarget = recoveryTarget;
    }

    @Override
    protected String transportAction() {
        return RecoveryAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected RecoveryRequest newRequest() {
        return new RecoveryRequest();
    }

    @Override
    protected RecoveryResponse newResponse(RecoveryRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {

        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        Map<String, List<ShardRecoveryResponse>> shardResponses = new HashMap<String, List<ShardRecoveryResponse>>();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<ShardOperationFailedException>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardRecoveryResponse recoveryResponse = (ShardRecoveryResponse) shardResponse;
                successfulShards++;

                if (recoveryResponse.recovering()) {
                    String indexName = recoveryResponse.recoveryMetrics().shardId().index().name();
                    List<ShardRecoveryResponse> responses = shardResponses.get(indexName);
                    if (responses == null) {
                        responses = new ArrayList<ShardRecoveryResponse>();
                        shardResponses.put(indexName, responses);
                    }
                    responses.add(recoveryResponse);
                }
            }
        }

        RecoveryResponse response = new RecoveryResponse(shardsResponses.length(), successfulShards, failedShards, shardResponses, shardFailures);
        return response;
    }

    @Override
    protected ShardRecoveryRequest newShardRequest() {
        return new ShardRecoveryRequest();
    }

    @Override
    protected ShardRecoveryRequest newShardRequest(ShardRouting shard, RecoveryRequest request) {
        return new ShardRecoveryRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardRecoveryResponse newShardResponse() {
        return new ShardRecoveryResponse();
    }

    @Override
    protected ShardRecoveryResponse shardOperation(ShardRecoveryRequest request) throws ElasticsearchException {

        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.index());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId());
        ShardRouting shardRouting = indexShard.routingEntry();
        ShardRecoveryResponse shardRecoveryResponse = new ShardRecoveryResponse(shardRouting.index(), shardRouting.id());

        // XXX - (andrew) - Should have a tighter coupling between index_shard_state and recovery_metrics.state; look into this
        boolean isShardRecovering = indexShard.state() == IndexShardState.RECOVERING;

        // Can remove isShardRecovering state variable

        shardRecoveryResponse.recovering(isShardRecovering);
        shardRecoveryResponse.state = indexShard.state();

        if (!isShardRecovering) {
            // Would get historical recovery data here.
            return shardRecoveryResponse;
        }

        RecoveryMetrics recoveryMetrics = null;

        RecoveryStatus recoveryStatus = indexShard.peerRecoveryStatus();
        if (recoveryStatus == null) {
            recoveryStatus = recoveryTarget.peerRecoveryStatus(indexShard.shardId());
        }

        if (recoveryStatus != null) {
            recoveryMetrics = recoveryStatus.metrics();
        } else {
            IndexShardGatewayService gatewayService =
                    indexService.shardInjector(request.shardId()).getInstance(IndexShardGatewayService.class);
            recoveryMetrics = gatewayService.recoveryMetrics();
        }

        shardRecoveryResponse.recoveryMetrics(recoveryMetrics);
        return shardRecoveryResponse;
    }


    // XXX - (andrew) - DON'T BROADCAST HERE UNLESS USER WANTS ALREADY RECOVERED SHARDS

    @Override
    protected GroupShardsIterator shards(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.routingTable().allAssignedShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, RecoveryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    public static class ShardRecoveryRequest extends BroadcastShardOperationRequest {

        ShardRecoveryRequest() { }

        ShardRecoveryRequest(String index, int shardId, RecoveryRequest request) {
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