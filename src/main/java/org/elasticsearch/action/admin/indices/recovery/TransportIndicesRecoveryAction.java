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
import org.elasticsearch.indices.IndicesService;
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
public class TransportIndicesRecoveryAction extends TransportBroadcastOperationAction<IndicesRecoveryRequest, IndicesRecoveryResponse, TransportIndicesRecoveryAction.IndexShardRecoveryRequest, RecoveryStats> {

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

                // XXX - DO SOMETHING HERE
                logger.debug(shardResponse.toString());

                successfulShards++;
            }
        }

        return new IndicesRecoveryResponse();
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
    protected RecoveryStats newShardResponse() {
        // NOCOMMIT - IMPLEMENT
        logger.debug("\n\nXXX NEW SHARD RESPONSE");
        return null;
    }

    @Override
    protected RecoveryStats shardOperation(IndexShardRecoveryRequest request) throws ElasticsearchException {
        // NOCOMMIT - IMPLEMENT
        logger.info("\n\nSHARD OPERATION - REQ: {}", request.toString());
        return new RecoveryStats();
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

        public IndexShardRecoveryRequest() {
        }

        public IndexShardRecoveryRequest(String index, int shardId, IndicesRecoveryRequest request) {
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
