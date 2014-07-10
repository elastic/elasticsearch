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

package org.elasticsearch.indices.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.service.IndexService;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.service.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class TransportShardActive extends AbstractComponent {

    public static final String ACTION_SHARD_EXISTS = "internal:index/shard/exists";
    private static final EnumSet<IndexShardState> ACTIVE_STATES = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED);

    private final ClusterService clusterService;
    private final IndicesService indicesService;
    private final TransportService transportService;

    @Inject
    public TransportShardActive(Settings settings, ClusterService clusterService, IndicesService indicesService, TransportService transportService) {
        super(settings);
        this.clusterService = clusterService;
        this.indicesService = indicesService;
        this.transportService = transportService;
        transportService.registerHandler(ACTION_SHARD_EXISTS, new ShardActiveRequestHandler());
    }

    public void shardActiveCount(ClusterState state, ShardId shardId, Iterable<ShardRouting> indexShardRoutingTable, ActionListener<Result> listener) {
        List<Tuple<DiscoveryNode, ShardActiveRequest>> requests = new ArrayList<>(4);
        String indexUUID = state.getMetaData().index(shardId.getIndex()).getUUID();
        ClusterName clusterName = state.getClusterName();
        for (ShardRouting shardRouting : indexShardRoutingTable) {
            DiscoveryNode currentNode = state.nodes().get(shardRouting.currentNodeId());
            if (currentNode != null) {
                requests.add(new Tuple<>(currentNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId())));
            }
            if (shardRouting.relocatingNodeId() != null) {
                DiscoveryNode relocatingNode = state.nodes().get(shardRouting.relocatingNodeId());
                if (relocatingNode != null) {
                    requests.add(new Tuple<>(relocatingNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId())));
                }
            }
        }

        ShardActiveResponseHandler responseHandler = new ShardActiveResponseHandler(shardId, requests.size(), listener);
        for (Tuple<DiscoveryNode, ShardActiveRequest> request : requests) {
            DiscoveryNode target = request.v1();
            if (clusterService.localNode().equals(target)) {
                responseHandler.handleResponse(new ShardActiveResponse(shardActive(request.v2()), target));
            } else {
                logger.trace("Sending shard exists request to node [{}] for shard [{}]", target, shardId);
                transportService.sendRequest(target, ACTION_SHARD_EXISTS, request.v2(), responseHandler);
            }
        }
    }

    private boolean shardActive(ShardActiveRequest request) {
        ClusterName thisClusterName = clusterService.state().getClusterName();
        if (!thisClusterName.equals(request.clusterName)) {
            logger.trace("shard exists request meant for cluster[{}], but this is cluster[{}], ignoring request", request.clusterName, thisClusterName);
            return false;
        }

        ShardId shardId = request.shardId;
        IndexService indexService = indicesService.indexService(shardId.index().getName());
        if (indexService != null && indexService.indexUUID().equals(request.indexUUID)) {
            IndexShard indexShard = indexService.shard(shardId.getId());
            if (indexShard != null) {
                return ACTIVE_STATES.contains(indexShard.state());
            }
        }
        return false;
    }

    public static class Result {

        private final int targetedShards;
        private final int activeShards;

        public Result(int targetedShards, int activeShards) {
            this.targetedShards = targetedShards;
            this.activeShards = activeShards;
        }

        public int getTargetedShards() {
            return targetedShards;
        }

        public int getActiveShards() {
            return activeShards;
        }
    }

    private static class ShardActiveRequest extends TransportRequest {

        private ClusterName clusterName;
        private String indexUUID;
        private ShardId shardId;

        ShardActiveRequest() {
        }

        ShardActiveRequest(ClusterName clusterName, String indexUUID, ShardId shardId) {
            this.shardId = shardId;
            this.indexUUID = indexUUID;
            this.clusterName = clusterName;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterName = ClusterName.readClusterName(in);
            indexUUID = in.readString();
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterName.writeTo(out);
            out.writeString(indexUUID);
            shardId.writeTo(out);
        }
    }

    private static class ShardActiveResponse extends TransportResponse {

        private boolean shardActive;
        private DiscoveryNode node;

        ShardActiveResponse() {
        }

        ShardActiveResponse(boolean shardActive, DiscoveryNode node) {
            this.shardActive = shardActive;
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardActive = in.readBoolean();
            node = DiscoveryNode.readNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(shardActive);
            node.writeTo(out);
        }
    }

    private class ShardActiveRequestHandler extends BaseTransportRequestHandler<ShardActiveRequest> {

        @Override
        public ShardActiveRequest newInstance() {
            return new ShardActiveRequest();
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        @Override
        public void messageReceived(ShardActiveRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(new ShardActiveResponse(shardActive(request), clusterService.localNode()));
        }
    }

    private class ShardActiveResponseHandler implements TransportResponseHandler<ShardActiveResponse> {

        private final ShardId shardId;
        private final int expectedActiveCopies;
        private final ActionListener<Result> listener;

        private final AtomicInteger awaitingResponses;
        private final AtomicInteger activeCopies;

        public ShardActiveResponseHandler(ShardId shardId, int expectedActiveCopies, ActionListener<Result> listener) {
            this.shardId = shardId;
            this.expectedActiveCopies = expectedActiveCopies;
            this.listener = listener;
            this.awaitingResponses = new AtomicInteger(expectedActiveCopies);
            this.activeCopies = new AtomicInteger();
        }

        @Override
        public ShardActiveResponse newInstance() {
            return new ShardActiveResponse();
        }

        @Override
        public void handleResponse(ShardActiveResponse response) {
            if (response.shardActive) {
                logger.trace("[{}] exists on node [{}]", shardId, response.node);
                activeCopies.incrementAndGet();
            }
            countDownAndFinish();
        }

        @Override
        public void handleException(TransportException exp) {
            logger.debug("shards active request failed for {}", exp, shardId);
            countDownAndFinish();
        }

        private void countDownAndFinish() {
            if (awaitingResponses.decrementAndGet() == 0) {
                listener.onResponse(new Result(expectedActiveCopies, activeCopies.get()));
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

    }

}
