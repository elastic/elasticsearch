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
package org.elasticsearch.action.admin.indices.shards;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.shards.IndicesUnassigedShardsResponse.ShardStatus;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Transport action that reads the cluster state for any unassigned shards for specific indices
 * and fetches the shard version, host node id and any exception while opening the shard index
 * from all the nodes.
 */
public class TransportIndicesUnassignedShardsAction extends TransportMasterNodeReadAction<IndicesUnassignedShardsRequest, IndicesUnassigedShardsResponse> {

    private final TransportNodesListGatewayStartedShards listShardsInfo;

    @Inject
    public TransportIndicesUnassignedShardsAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                                  TransportNodesListGatewayStartedShards listShardsInfo) {
        super(settings, IndicesUnassignedShardsAction.NAME, transportService, clusterService, threadPool, actionFilters, IndicesUnassignedShardsRequest.class);
        this.listShardsInfo = listShardsInfo;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesUnassigedShardsResponse newResponse() {
        return new IndicesUnassigedShardsResponse();
    }

    @Override
    protected void masterOperation(IndicesUnassignedShardsRequest request, ClusterState state, ActionListener<IndicesUnassigedShardsResponse> listener) {
        // collect all unassigned shards for the requested indices
        DiscoveryNodes nodes = state.nodes();
        MetaData metaData = state.metaData();
        Set<String> requestedIndices = new HashSet<>();
        requestedIndices.addAll(Arrays.asList(request.indices()));
        List<ShardId> shardIdsToFetch = new ArrayList<>(state.routingNodes().unassigned().size());
        for (MutableShardRouting shard : state.routingNodes().unassigned()) {
            if (requestedIndices.size() == 0 || requestedIndices.contains(shard.shardId().index().getName())) {
                shardIdsToFetch.add(shard.shardId());
            }
        }

        // async fetch shard status from all the nodes
        if (shardIdsToFetch.size() > 0) {
            AsyncShardFetches asyncShardFetches = new AsyncShardFetches(nodes, metaData, shardIdsToFetch.size(), listener);
            asyncShardFetches.fetch(shardIdsToFetch);
        } else {
            listener.onResponse(new IndicesUnassigedShardsResponse());
        }
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesUnassignedShardsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, request.indices());
    }

    private class AsyncShardFetches {
        private final DiscoveryNodes nodes;
        private final MetaData metaData;
        private final int expectedOps;
        private final ActionListener<IndicesUnassigedShardsResponse> listener;
        private final AtomicInteger opsCount;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardFetches(DiscoveryNodes nodes, MetaData metaData, int expectedOps, ActionListener<IndicesUnassigedShardsResponse> listener) {
            this.nodes = nodes;
            this.metaData = metaData;
            this.listener = listener;
            this.expectedOps = expectedOps;
            this.opsCount = new AtomicInteger(0);
            this.fetchResponses = new ConcurrentLinkedQueue<>();
        }

        void fetch(Collection<ShardId> shardIds) {
            for (ShardId shardId : shardIds) {
                InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "monitor_shard_copies", shardId, listShardsInfo);
                fetch.fetchData(nodes, metaData, Collections.<String>emptySet());
            }
        }

        private class InternalAsyncFetch extends AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> {

            InternalAsyncFetch(ESLogger logger, String type, ShardId shardId, TransportNodesListGatewayStartedShards action) {
                super(logger, type, shardId, action);
            }

            @Override
            protected synchronized void processAsyncFetch(ShardId shardId, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                super.processAsyncFetch(shardId, responses, failures);
                fetchResponses.add(new Response(shardId, responses, failures));
                if (opsCount.incrementAndGet() == expectedOps) {
                    finish();
                }
            }

            void finish() {
                ImmutableOpenMap.Builder<String, Map<Integer, java.util.List<ShardStatus>>> resBuilder = ImmutableOpenMap.builder();
                while (!fetchResponses.isEmpty()) {
                    Response res = fetchResponses.remove();
                    Map<Integer, java.util.List<ShardStatus>> shardMap = resBuilder.get(res.shardId.getIndex());
                    if (shardMap == null) {
                        shardMap = new HashMap<>();
                    }
                    java.util.List<ShardStatus> shardDataList = shardMap.get(res.shardId.id());
                    if (shardDataList == null) {
                        shardDataList = new ArrayList<>();
                    }
                    for (TransportNodesListGatewayStartedShards.NodeGatewayStartedShards response : res.responses) {
                        shardDataList.add(new ShardStatus(response.getNode().id(), response.version(), response.exception()));
                    }
                    shardMap.put(res.shardId.id(), shardDataList);
                    resBuilder.put(res.shardId.getIndex(), shardMap);
                }
                listener.onResponse(new IndicesUnassigedShardsResponse(resBuilder.build()));
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            public class Response {
                private final ShardId shardId;
                private final TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses;
                private final FailedNodeException[] failures;

                public Response(ShardId shardId, TransportNodesListGatewayStartedShards.NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }
}
