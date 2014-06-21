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
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Transport action for shard recovery operation. This transport action does not actually
 * perform shard recovery, it only reports on recoveries (both active and complete).
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
        Map<String, List<ShardRecoveryResponse>> shardResponses = new HashMap<>();

        for (int i = 0; i < shardsResponses.length(); i++) {
            Object shardResponse = shardsResponses.get(i);
            if (shardResponse == null) {
                // simply ignore non active shards
            } else if (shardResponse instanceof BroadcastShardOperationFailedException) {
                failedShards++;
                if (shardFailures == null) {
                    shardFailures = new ArrayList<>();
                }
                shardFailures.add(new DefaultShardOperationFailedException((BroadcastShardOperationFailedException) shardResponse));
            } else {
                ShardRecoveryResponse recoveryResponse = (ShardRecoveryResponse) shardResponse;
                successfulShards++;
                String indexName = recoveryResponse.getIndex();
                List<ShardRecoveryResponse> responses = shardResponses.get(indexName);

                if (responses == null) {
                    responses = new ArrayList<>();
                    shardResponses.put(indexName, responses);
                }

                if (request.activeOnly()) {
                    if (recoveryResponse.recoveryState().getStage() != RecoveryState.Stage.DONE) {
                        responses.add(recoveryResponse);
                    }
                } else {
                    responses.add(recoveryResponse);
                }
            }
        }

        RecoveryResponse response = new RecoveryResponse(shardsResponses.length(), successfulShards,
                failedShards, request.detailed(), shardResponses, shardFailures);
        return response;
    }

    @Override
    protected ShardRecoveryRequest newShardRequest() {
        return new ShardRecoveryRequest();
    }

    @Override
    protected ShardRecoveryRequest newShardRequest(int numShards, ShardRouting shard, RecoveryRequest request) {
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

        RecoveryState state;
        RecoveryStatus recoveryStatus = indexShard.recoveryStatus();

        if (recoveryStatus == null) {
            recoveryStatus = recoveryTarget.recoveryStatus(indexShard.shardId());
        }

        if (recoveryStatus != null) {
            state = recoveryStatus.recoveryState();
        } else {
            IndexShardGatewayService gatewayService =
                    indexService.shardInjector(request.shardId()).getInstance(IndexShardGatewayService.class);
            state = gatewayService.recoveryState();
        }

        shardRecoveryResponse.recoveryState(state);
        return shardRecoveryResponse;
    }

    @Override
    protected GroupShardsIterator shards(ClusterState state, RecoveryRequest request, String[] concreteIndices) {
        return state.routingTable().allAssignedShardsGrouped(concreteIndices, true, true);
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