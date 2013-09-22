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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.ElasticSearchException;
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
import org.elasticsearch.index.engine.Engine;
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
public class TransportIndicesStatusAction extends TransportBroadcastOperationAction<IndicesStatusRequest, IndicesStatusResponse, TransportIndicesStatusAction.IndexShardStatusRequest, ShardStatus> {

    private final IndicesService indicesService;

    private final RecoveryTarget peerRecoveryTarget;

    @Inject
    public TransportIndicesStatusAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, RecoveryTarget peerRecoveryTarget) {
        super(settings, threadPool, clusterService, transportService);
        this.peerRecoveryTarget = peerRecoveryTarget;
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected String transportAction() {
        return IndicesStatusAction.NAME;
    }

    @Override
    protected IndicesStatusRequest newRequest() {
        return new IndicesStatusRequest();
    }

    /**
     * Status goes across *all* shards.
     */
    @Override
    protected GroupShardsIterator shards(ClusterState state, IndicesStatusRequest request, String[] concreteIndices) {
        return state.routingTable().allAssignedShardsGrouped(concreteIndices, true);
    }

    @Override
    protected ClusterBlockException checkGlobalBlock(ClusterState state, IndicesStatusRequest request) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }

    @Override
    protected ClusterBlockException checkRequestBlock(ClusterState state, IndicesStatusRequest countRequest, String[] concreteIndices) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA, concreteIndices);
    }

    @Override
    protected IndicesStatusResponse newResponse(IndicesStatusRequest request, AtomicReferenceArray shardsResponses, ClusterState clusterState) {
        int successfulShards = 0;
        int failedShards = 0;
        List<ShardOperationFailedException> shardFailures = null;
        final List<ShardStatus> shards = newArrayList();
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
                shards.add((ShardStatus) shardResponse);
                successfulShards++;
            }
        }
        return new IndicesStatusResponse(shards.toArray(new ShardStatus[shards.size()]), clusterState, shardsResponses.length(), successfulShards, failedShards, shardFailures);
    }

    @Override
    protected IndexShardStatusRequest newShardRequest() {
        return new IndexShardStatusRequest();
    }

    @Override
    protected IndexShardStatusRequest newShardRequest(ShardRouting shard, IndicesStatusRequest request) {
        return new IndexShardStatusRequest(shard.index(), shard.id(), request);
    }

    @Override
    protected ShardStatus newShardResponse() {
        return new ShardStatus();
    }

    @Override
    protected ShardStatus shardOperation(IndexShardStatusRequest request) throws ElasticSearchException {
        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.index());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId());
        ShardStatus shardStatus = new ShardStatus(indexShard.routingEntry());
        shardStatus.state = indexShard.state();
        try {
            shardStatus.storeSize = indexShard.store().estimateSize();
        } catch (IOException e) {
            // failure to get the store size...
        }
        if (indexShard.state() == IndexShardState.STARTED) {
//            shardStatus.estimatedFlushableMemorySize = indexShard.estimateFlushableMemorySize();
            shardStatus.translogId = indexShard.translog().currentId();
            shardStatus.translogOperations = indexShard.translog().estimatedNumberOfOperations();
            Engine.Searcher searcher = indexShard.acquireSearcher("indices_status");
            try {
                shardStatus.docs = new DocsStatus();
                shardStatus.docs.numDocs = searcher.reader().numDocs();
                shardStatus.docs.maxDoc = searcher.reader().maxDoc();
                shardStatus.docs.deletedDocs = searcher.reader().numDeletedDocs();
            } finally {
                searcher.release();
            }

            shardStatus.mergeStats = indexShard.mergeScheduler().stats();
            shardStatus.refreshStats = indexShard.refreshStats();
            shardStatus.flushStats = indexShard.flushStats();
        }

        if (request.recovery) {
            // check on going recovery (from peer or gateway)
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
                shardStatus.peerRecoveryStatus = new PeerRecoveryStatus(stage, peerRecoveryStatus.startTime(), peerRecoveryStatus.time(),
                        peerRecoveryStatus.phase1TotalSize(), peerRecoveryStatus.phase1ExistingTotalSize(),
                        peerRecoveryStatus.currentFilesSize(), peerRecoveryStatus.currentTranslogOperations());
            }

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
                shardStatus.gatewayRecoveryStatus = new GatewayRecoveryStatus(stage, gatewayRecoveryStatus.startTime(), gatewayRecoveryStatus.time(),
                        gatewayRecoveryStatus.index().totalSize(), gatewayRecoveryStatus.index().reusedTotalSize(), gatewayRecoveryStatus.index().currentFilesSize(), gatewayRecoveryStatus.translog().currentTranslogOperations());
            }
        }

        if (request.snapshot) {
            IndexShardGatewayService gatewayService = indexService.shardInjector(request.shardId()).getInstance(IndexShardGatewayService.class);
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
                shardStatus.gatewaySnapshotStatus = new GatewaySnapshotStatus(stage, snapshotStatus.startTime(), snapshotStatus.time(),
                        snapshotStatus.index().totalSize(), snapshotStatus.translog().expectedNumberOfOperations());
            }
        }

        return shardStatus;
    }

    public static class IndexShardStatusRequest extends BroadcastShardOperationRequest {

        boolean recovery;

        boolean snapshot;

        IndexShardStatusRequest() {
        }

        IndexShardStatusRequest(String index, int shardId, IndicesStatusRequest request) {
            super(index, shardId, request);
            recovery = request.recovery();
            snapshot = request.snapshot();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            recovery = in.readBoolean();
            snapshot = in.readBoolean();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(recovery);
            out.writeBoolean(snapshot);
        }
    }
}
