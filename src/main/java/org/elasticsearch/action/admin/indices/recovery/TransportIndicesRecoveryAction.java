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
import org.elasticsearch.action.admin.indices.status.GatewayRecoveryStatus;
import org.elasticsearch.action.admin.indices.status.GatewaySnapshotStatus;
import org.elasticsearch.action.admin.indices.status.PeerRecoveryStatus;
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
import org.elasticsearch.index.gateway.SnapshotStatus;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
 */
public class TransportIndicesRecoveryAction extends TransportBroadcastOperationAction<IndicesRecoveryRequest, IndicesRecoveryResponse, TransportIndicesRecoveryAction.IndexShardRecoveryRequest, ShardRecoveryStatus> {

    private final IndicesService indicesService;
    private final RecoveryTarget peerRecoveryTarget;

    @Inject
    public TransportIndicesRecoveryAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                             TransportService transportService, IndicesService indicesService,
                                             RecoveryTarget peerRecoveryTarget) {
        super(settings, threadPool, clusterService, transportService);
        this.indicesService = indicesService;
        this.peerRecoveryTarget = peerRecoveryTarget;
    }

    @Override
    protected String transportAction() {
        return IndicesRecoveryAction.NAME;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected IndicesRecoveryRequest newRequest() {
        return new IndicesRecoveryRequest();
    }

    @Override
    protected IndicesRecoveryResponse newResponse(IndicesRecoveryRequest request, AtomicReferenceArray shardsResponses,
                                                  ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        List<ShardRecoveryStatus> shardRecoveries = newArrayList();

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
                shardRecoveries.add((ShardRecoveryStatus) shardResponse);
                successfulShards++;
            }
        }

        return new IndicesRecoveryResponse(shardRecoveries.toArray(new ShardRecoveryStatus[shardRecoveries.size()]),
                clusterState, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndexShardRecoveryRequest newShardRequest() {
        return new IndexShardRecoveryRequest();
    }

    @Override
    protected IndexShardRecoveryRequest newShardRequest(ShardRouting shard, IndicesRecoveryRequest request) {
        return new IndexShardRecoveryRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardRecoveryStatus newShardResponse() {
        return new ShardRecoveryStatus();
    }

    @Override
    protected ShardRecoveryStatus shardOperation(IndexShardRecoveryRequest request) throws ElasticsearchException {

        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.index());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId());
        ShardRecoveryStatus recoveryStatus = new ShardRecoveryStatus(indexShard.routingEntry());

        recoveryStatus.recovering((indexShard.state() == IndexShardState.RECOVERING) ? true : false);

        try {
            recoveryStatus.estimatedStoreSize(indexShard.store().estimateSize());
        } catch (IOException e) {
            logger.warn("Unable to read estimated store size for index: {} shard: {}", request.index(), request.shardId());
        }

        // Check for on-going peer recovery.
        RecoveryStatus peerRecoveryStatus = indexShard.peerRecoveryStatus();
        if (peerRecoveryStatus == null) {
            peerRecoveryStatus = peerRecoveryTarget.peerRecoveryStatus(indexShard.shardId());
        }
        if (peerRecoveryStatus != null) {
            PeerRecoveryStatus.Stage stage;
            switch (peerRecoveryStatus.stage()) {
                case INIT:
                    stage = PeerRecoveryStatus.Stage.INIT;
                    break;
                case INDEX:
                    stage = PeerRecoveryStatus.Stage.INDEX;
                    break;
                case TRANSLOG:
                    stage = PeerRecoveryStatus.Stage.TRANSLOG;
                    break;
                case FINALIZE:
                    stage = PeerRecoveryStatus.Stage.FINALIZE;
                    break;
                case DONE:
                    stage = PeerRecoveryStatus.Stage.DONE;
                    break;
                default:
                    stage = PeerRecoveryStatus.Stage.INIT;
            }

            recoveryStatus.peerRecoveryStatus(new PeerRecoveryStatus(stage, peerRecoveryStatus.startTime(), peerRecoveryStatus.time(),
                    peerRecoveryStatus.phase1TotalSize(), peerRecoveryStatus.phase1ExistingTotalSize(),
                    peerRecoveryStatus.currentFilesSize(), peerRecoveryStatus.currentTranslogOperations()));
        }

        // Check for on-going gateway recovery.
        IndexShardGatewayService gatewayService = indexService.shardInjector(request.shardId()).getInstance(IndexShardGatewayService.class);
        org.elasticsearch.index.gateway.RecoveryStatus gatewayRecoveryStatus = gatewayService.recoveryStatus();
        if (gatewayRecoveryStatus != null) {
            GatewayRecoveryStatus.Stage stage;
            switch (gatewayRecoveryStatus.stage()) {
                case INIT:
                    stage = GatewayRecoveryStatus.Stage.INIT;
                    break;
                case INDEX:
                    stage = GatewayRecoveryStatus.Stage.INDEX;
                    break;
                case TRANSLOG:
                    stage = GatewayRecoveryStatus.Stage.TRANSLOG;
                    break;
                case DONE:
                    stage = GatewayRecoveryStatus.Stage.DONE;
                    break;
                default:
                    stage = GatewayRecoveryStatus.Stage.INIT;
            }

            recoveryStatus.gatewayRecoveryStatus(new GatewayRecoveryStatus(stage, gatewayRecoveryStatus.startTime(), gatewayRecoveryStatus.time(),
                    gatewayRecoveryStatus.index().totalSize(), gatewayRecoveryStatus.index().reusedTotalSize(),
                    gatewayRecoveryStatus.index().currentFilesSize(), gatewayRecoveryStatus.translog().currentTranslogOperations()));
        }

        // Check for on-going gateway snapshot recovery
        SnapshotStatus snapshotStatus = gatewayService.snapshotStatus();
        if (snapshotStatus != null) {
            GatewaySnapshotStatus.Stage stage;
            switch (snapshotStatus.stage()) {
                case DONE:
                    stage = GatewaySnapshotStatus.Stage.DONE;
                    break;
                case FAILURE:
                    stage = GatewaySnapshotStatus.Stage.FAILURE;
                    break;
                case TRANSLOG:
                    stage = GatewaySnapshotStatus.Stage.TRANSLOG;
                    break;
                case FINALIZE:
                    stage = GatewaySnapshotStatus.Stage.FINALIZE;
                    break;
                case INDEX:
                    stage = GatewaySnapshotStatus.Stage.INDEX;
                    break;
                default:
                    stage = GatewaySnapshotStatus.Stage.NONE;
                    break;
            }

            recoveryStatus.gatewaySnapshotStatus(new GatewaySnapshotStatus(stage, snapshotStatus.startTime(), snapshotStatus.time(),
                    snapshotStatus.index().totalSize(), snapshotStatus.translog().expectedNumberOfOperations()));
        }

        return recoveryStatus;
    }

    /**
     * Request for recovery information goes across *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState state, IndicesRecoveryRequest request, String[] concreteIndices) {
        return state.routingTable().allAssignedShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesRecoveryRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesRecoveryRequest request, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    public static class IndexShardRecoveryRequest extends BroadcastShardOperationRequest {

        IndexShardRecoveryRequest() {
        }

        IndexShardRecoveryRequest(String index, int shardId, IndicesRecoveryRequest request) {
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
