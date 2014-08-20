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

package org.elasticsearch.action.admin.indices.status;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.support.ActionFilters;
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
import org.elasticsearch.common.lucene.Directories;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.gateway.IndexShardGatewayService;
import org.elasticsearch.index.service.InternalIndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.InternalIndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.indices.recovery.RecoveryStatus;
import org.elasticsearch.indices.recovery.RecoveryTarget;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.google.common.collect.Lists.newArrayList;

/**
 * This class will be removed in future versions
 * Use the recovery API instead
 */
@Deprecated
public class TransportIndicesStatusAction extends TransportBroadcastOperationAction<IndicesStatusRequest, IndicesStatusResponse, TransportIndicesStatusAction.IndexShardStatusRequest, ShardStatus> {

    private final IndicesService indicesService;

    private final RecoveryTarget peerRecoveryTarget;

    @Inject
    public TransportIndicesStatusAction(Settings settings, ThreadPool threadPool, ClusterService clusterService, TransportService transportService,
                                        IndicesService indicesService, RecoveryTarget peerRecoveryTarget, ActionFilters actionFilters) {
        super(settings, IndicesStatusAction.NAME, threadPool, clusterService, transportService, actionFilters);
        this.peerRecoveryTarget = peerRecoveryTarget;
        this.indicesService = indicesService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
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
    protected IndexShardStatusRequest newShardRequest(int numShards, ShardRouting shard, IndicesStatusRequest request) {
        return new IndexShardStatusRequest(shard.shardId(), request);
    }

    @Override
    protected ShardStatus newShardResponse() {
        return new ShardStatus();
    }

    @Override
    protected ShardStatus shardOperation(IndexShardStatusRequest request) throws ElasticsearchException {
        InternalIndexService indexService = (InternalIndexService) indicesService.indexServiceSafe(request.shardId().getIndex());
        InternalIndexShard indexShard = (InternalIndexShard) indexService.shardSafe(request.shardId().id());
        ShardStatus shardStatus = new ShardStatus(indexShard.routingEntry());
        shardStatus.state = indexShard.state();
        final Store store = indexShard.store();
        store.incRef();
        try {
            shardStatus.storeSize = new ByteSizeValue(Directories.estimateSize(store.directory()));
        } catch (IOException e) {
            // failure to get the store size...
        } finally {
            store.decRef();
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
                searcher.close();
            }

            shardStatus.mergeStats = indexShard.mergeScheduler().stats();
            shardStatus.refreshStats = indexShard.refreshStats();
            shardStatus.flushStats = indexShard.flushStats();
        }

        if (request.recovery) {
            // check on going recovery (from peer or gateway)
            RecoveryStatus peerRecoveryStatus = indexShard.recoveryStatus();
            if (peerRecoveryStatus == null) {
                peerRecoveryStatus = peerRecoveryTarget.recoveryStatus(indexShard);
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
                shardStatus.peerRecoveryStatus = new PeerRecoveryStatus(stage, peerRecoveryStatus.recoveryState().getTimer().startTime(),
                        peerRecoveryStatus.recoveryState().getTimer().time(),
                        peerRecoveryStatus.recoveryState().getIndex().totalByteCount(),
                        peerRecoveryStatus.recoveryState().getIndex().reusedByteCount(),
                        peerRecoveryStatus.recoveryState().getIndex().recoveredByteCount(), peerRecoveryStatus.recoveryState().getTranslog().currentTranslogOperations());
            }

            IndexShardGatewayService gatewayService = indexService.shardInjector(request.shardId().id()).getInstance(IndexShardGatewayService.class);
            RecoveryState gatewayRecoveryState = gatewayService.recoveryState();
            if (gatewayRecoveryState != null) {
                GatewayRecoveryStatus.Stage stage;
                switch (gatewayRecoveryState.getStage()) {
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
                shardStatus.gatewayRecoveryStatus = new GatewayRecoveryStatus(stage, gatewayRecoveryState.getTimer().startTime(), gatewayRecoveryState.getTimer().time(),
                        gatewayRecoveryState.getIndex().totalByteCount(), gatewayRecoveryState.getIndex().reusedByteCount(), gatewayRecoveryState.getIndex().recoveredByteCount(), gatewayRecoveryState.getTranslog().currentTranslogOperations());
            }
        }
        return shardStatus;
    }

    static class IndexShardStatusRequest extends BroadcastShardOperationRequest {

        boolean recovery;

        boolean snapshot;

        IndexShardStatusRequest() {
        }

        IndexShardStatusRequest(ShardId shardId, IndicesStatusRequest request) {
            super(shardId, request);
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
