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

import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.action.admin.indices.shards.IndicesShardsStoresRequest.*;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link Status}) of specific indices
 * and fetches shard store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardsStoresAction extends TransportMasterNodeReadAction<IndicesShardsStoresRequest, IndicesShardsStoresResponse> {

    private final TransportNodesListGatewayStartedShards listShardStoresInfo;

    @Inject
    public TransportIndicesShardsStoresAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                              TransportNodesListGatewayStartedShards listShardStoresInfo) {
        super(settings, IndicesShardsStoresAction.NAME, transportService, clusterService, threadPool, actionFilters, IndicesShardsStoresRequest.class);
        this.listShardStoresInfo = listShardStoresInfo;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesShardsStoresResponse newResponse() {
        return new IndicesShardsStoresResponse();
    }

    @Override
    protected void masterOperation(IndicesShardsStoresRequest request, ClusterState state, ActionListener<IndicesShardsStoresResponse> listener) {
        final Set<ShardRoutingState> routingStates = new HashSet<>(request.shardStatuses().length);
        for (Status status : request.shardStatuses()) {
            switch (status) {
                case GREEN:
                case ALL:
                    routingStates.addAll(Arrays.asList(ShardRoutingState.values()));
                    break;
                case YELLOW:
                case RED:
                    routingStates.add(ShardRoutingState.UNASSIGNED);
                    break;
                default:
                    // bogus
                    listener.onFailure(new IllegalArgumentException("Invalid status criteria"));
                    return;
            }
        }
        // collect relevant shard ids of the requested indices for fetching store infos
        final RoutingTable routingTables = state.routingTable();
        final Set<ShardId> shardIdsToFetch = new HashSet<>();
        for (ShardRoutingState routingState : routingStates) {
            for (String concreteIndex : state.metaData().concreteIndices(request.indicesOptions(), request.indices())) {
                for (ShardRouting shardRouting : routingTables.index(concreteIndex).shardsWithState(routingState)) {
                    shardIdsToFetch.add(shardRouting.shardId());
                }
            }
        }
        // async fetch store infos from all the nodes
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard
        new AsyncShardsStoresInfoFetches(state.nodes(), routingTables, state.metaData(), shardIdsToFetch, listener).start();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardsStoresRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, state.metaData().concreteIndices(request.indicesOptions(), request.indices()));
    }

    private class AsyncShardsStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingTable routingTables;
        private final MetaData metaData;
        private final Set<ShardId> shardIds;
        private final ActionListener<IndicesShardsStoresResponse> listener;
        private CountDown expectedOps;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardsStoresInfoFetches(DiscoveryNodes nodes, RoutingTable routingTables, MetaData metaData, Set<ShardId> shardIds, ActionListener<IndicesShardsStoresResponse> listener) {
            this.nodes = nodes;
            this.routingTables = routingTables;
            this.metaData = metaData;
            this.shardIds = shardIds;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
            this.expectedOps = new CountDown(shardIds.size());
        }

        void start() {
            if (shardIds.isEmpty()) {
                listener.onResponse(new IndicesShardsStoresResponse());
            } else {
                for (ShardId shardId : shardIds) {
                    InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "shard_stores", shardId, listShardStoresInfo);
                    fetch.fetchData(nodes, metaData, Collections.<String>emptySet());
                }
            }
        }

        private class InternalAsyncFetch extends AsyncShardFetch<NodeGatewayStartedShards> {

            InternalAsyncFetch(ESLogger logger, String type, ShardId shardId, TransportNodesListGatewayStartedShards action) {
                super(logger, type, shardId, action);
            }

            @Override
            protected synchronized void processAsyncFetch(ShardId shardId, NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                fetchResponses.add(new Response(shardId, responses, failures));
                if (expectedOps.countDown()) {
                    finish();
                }
            }

            void finish() {
                Map<String, IntObjectHashMap<java.util.List<NodeGatewayStartedShards>>> shardsResponses = new HashMap<>();
                ImmutableList.Builder<IndicesShardsStoresResponse.Failure> failures = ImmutableList.builder();
                while (!fetchResponses.isEmpty()) {
                    Response res = fetchResponses.remove();
                    IntObjectHashMap<java.util.List<NodeGatewayStartedShards>> indexShards = shardsResponses.get(res.shardId.getIndex());
                    final IntObjectHashMap<java.util.List<NodeGatewayStartedShards>> indexShardsBuilder;
                    if (indexShards == null) {
                        indexShardsBuilder = new IntObjectHashMap<>();
                    } else {
                        indexShardsBuilder = new IntObjectHashMap<>(indexShards);
                    }
                    java.util.List<NodeGatewayStartedShards> shardStoreStatuses = indexShardsBuilder.get(res.shardId.id());
                    if (shardStoreStatuses == null) {
                        shardStoreStatuses = new ArrayList<>();
                    }
                    for (NodeGatewayStartedShards response : res.responses) {
                        if (shardExistsInNode(response)) {
                            shardStoreStatuses.add(response);
                        }
                    }
                    indexShardsBuilder.put(res.shardId.id(), shardStoreStatuses);
                    shardsResponses.put(res.shardId.getIndex(), indexShardsBuilder);
                    for (FailedNodeException failure : res.failures) {
                        failures.add(new IndicesShardsStoresResponse.Failure(failure.nodeId(), res.shardId.getIndex(), res.shardId.id(), failure.getCause()));
                    }
                }
                ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<java.util.List<IndicesShardsStoresResponse.ShardStoreStatus>>> indicesShardStoreStatuses = ImmutableOpenMap.builder();
                for (Map.Entry<String, IntObjectHashMap<java.util.List<NodeGatewayStartedShards>>> entry : shardsResponses.entrySet()) {
                    String index = entry.getKey();
                    ImmutableOpenIntMap.Builder<java.util.List<IndicesShardsStoresResponse.ShardStoreStatus>> shardStoreResponses = ImmutableOpenIntMap.builder();
                    for (IntObjectCursor<java.util.List<NodeGatewayStartedShards>> shardResponse : entry.getValue()) {
                        java.util.List<IndicesShardsStoresResponse.ShardStoreStatus> shardStoreStatusList = new ArrayList<>(shardResponse.value.size());
                        for (int i = 0; i < shardResponse.value.size(); i++) {
                            NodeGatewayStartedShards res = shardResponse.value.get(i);
                            final IndicesShardsStoresResponse.ShardStoreStatus.Allocation allocation;
                            if (res.storeException() == null) {
                                allocation = isPrimary(index, shardResponse.key, res.getNode())
                                        ? IndicesShardsStoresResponse.ShardStoreStatus.Allocation.PRIMARY
                                        : IndicesShardsStoresResponse.ShardStoreStatus.Allocation.REPLICA;
                            } else {
                                allocation = IndicesShardsStoresResponse.ShardStoreStatus.Allocation.UNUSED;
                            }
                            shardStoreStatusList.add(new IndicesShardsStoresResponse.ShardStoreStatus(res.getNode(), res.version(), allocation, res.storeException()));
                        }
                        Collections.sort(shardStoreStatusList);
                        shardStoreResponses.put(shardResponse.key, shardStoreStatusList);
                    }
                    indicesShardStoreStatuses.put(index, shardStoreResponses.build());
                }
                listener.onResponse(new IndicesShardsStoresResponse(indicesShardStoreStatuses.build(), failures.build()));
            }

            private boolean isPrimary(String index, int shardID, DiscoveryNode node) {
                IndexRoutingTable indexRoutingTable = routingTables.index(index);
                for (ShardRouting shardRouting : indexRoutingTable.shard(shardID)) {
                    if (shardRouting.assignedToNode() && shardRouting.currentNodeId().equals(node.getId())) {
                        return shardRouting.primary();
                    }
                }
                return false;
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             */
            private boolean shardExistsInNode(final NodeGatewayStartedShards response) {
                return response.storeException() != null || response.version() != -1;
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            public class Response {
                private final ShardId shardId;
                private final NodeGatewayStartedShards[] responses;
                private final FailedNodeException[] failures;

                public Response(ShardId shardId, NodeGatewayStartedShards[] responses, FailedNodeException[] failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }
}
