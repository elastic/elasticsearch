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

import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus;
import org.elasticsearch.action.admin.cluster.health.ClusterShardHealth;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific indices
 * and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardStoresAction extends TransportMasterNodeReadAction<IndicesShardStoresRequest, IndicesShardStoresResponse> {

    private final TransportNodesListGatewayStartedShards listShardStoresInfo;

    @Inject
    public TransportIndicesShardStoresAction(Settings settings, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver, TransportNodesListGatewayStartedShards listShardStoresInfo) {
        super(settings, IndicesShardStoresAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver, IndicesShardStoresRequest::new);
        this.listShardStoresInfo = listShardStoresInfo;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected IndicesShardStoresResponse newResponse() {
        return new IndicesShardStoresResponse();
    }

    @Override
    protected void masterOperation(IndicesShardStoresRequest request, ClusterState state, ActionListener<IndicesShardStoresResponse> listener) {
        final RoutingTable routingTables = state.routingTable();
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, request);
        final Set<ShardId> shardIdsToFetch = new HashSet<>();

        logger.trace("using cluster state version [{}] to determine shards", state.version());
        // collect relevant shard ids of the requested indices for fetching store infos
        for (String index : concreteIndices) {
            IndexRoutingTable indexShardRoutingTables = routingTables.index(index);
            if (indexShardRoutingTables == null) {
                continue;
            }
            for (IndexShardRoutingTable routing : indexShardRoutingTables) {
                ClusterShardHealth shardHealth = new ClusterShardHealth(routing.shardId().id(), routing);
                if (request.shardStatuses().contains(shardHealth.getStatus())) {
                    shardIdsToFetch.add(routing.shardId());
                }
            }
        }

        // async fetch store infos from all the nodes
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard
        new AsyncShardStoresInfoFetches(state.nodes(), routingNodes, state.metaData(), shardIdsToFetch, listener).start();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardStoresRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndices(state, request));
    }

    private class AsyncShardStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingNodes routingNodes;
        private final MetaData metaData;
        private final Set<ShardId> shardIds;
        private final ActionListener<IndicesShardStoresResponse> listener;
        private CountDown expectedOps;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardStoresInfoFetches(DiscoveryNodes nodes, RoutingNodes routingNodes, MetaData metaData, Set<ShardId> shardIds, ActionListener<IndicesShardStoresResponse> listener) {
            this.nodes = nodes;
            this.routingNodes = routingNodes;
            this.metaData = metaData;
            this.shardIds = shardIds;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
            this.expectedOps = new CountDown(shardIds.size());
        }

        void start() {
            if (shardIds.isEmpty()) {
                listener.onResponse(new IndicesShardStoresResponse());
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
                ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>>> indicesStoreStatusesBuilder = ImmutableOpenMap.builder();
                java.util.List<IndicesShardStoresResponse.Failure> failureBuilder = new ArrayList<>();
                for (Response fetchResponse : fetchResponses) {
                    ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexStoreStatuses = indicesStoreStatusesBuilder.get(fetchResponse.shardId.getIndex());
                    final ImmutableOpenIntMap.Builder<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexShardsBuilder;
                    if (indexStoreStatuses == null) {
                        indexShardsBuilder = ImmutableOpenIntMap.builder();
                    } else {
                        indexShardsBuilder = ImmutableOpenIntMap.builder(indexStoreStatuses);
                    }
                    java.util.List<IndicesShardStoresResponse.StoreStatus> storeStatuses = indexShardsBuilder.get(fetchResponse.shardId.id());
                    if (storeStatuses == null) {
                        storeStatuses = new ArrayList<>();
                    }
                    for (NodeGatewayStartedShards response : fetchResponse.responses) {
                        if (shardExistsInNode(response)) {
                            IndicesShardStoresResponse.StoreStatus.Allocation allocation = getAllocation(fetchResponse.shardId.getIndex(), fetchResponse.shardId.id(), response.getNode());
                            storeStatuses.add(new IndicesShardStoresResponse.StoreStatus(response.getNode(), response.version(), allocation, response.storeException()));
                        }
                    }
                    CollectionUtil.timSort(storeStatuses);
                    indexShardsBuilder.put(fetchResponse.shardId.id(), storeStatuses);
                    indicesStoreStatusesBuilder.put(fetchResponse.shardId.getIndex(), indexShardsBuilder.build());
                    for (FailedNodeException failure : fetchResponse.failures) {
                        failureBuilder.add(new IndicesShardStoresResponse.Failure(failure.nodeId(), fetchResponse.shardId.getIndex(), fetchResponse.shardId.id(), failure.getCause()));
                    }
                }
                listener.onResponse(new IndicesShardStoresResponse(indicesStoreStatusesBuilder.build(), Collections.unmodifiableList(failureBuilder)));
            }

            private IndicesShardStoresResponse.StoreStatus.Allocation getAllocation(String index, int shardID, DiscoveryNode node) {
                for (ShardRouting shardRouting : routingNodes.node(node.id())) {
                    ShardId shardId = shardRouting.shardId();
                    if (shardId.id() == shardID && shardId.getIndex().equals(index)) {
                        if (shardRouting.primary()) {
                            return IndicesShardStoresResponse.StoreStatus.Allocation.PRIMARY;
                        } else if (shardRouting.assignedToNode()) {
                            return IndicesShardStoresResponse.StoreStatus.Allocation.REPLICA;
                        } else {
                            return IndicesShardStoresResponse.StoreStatus.Allocation.UNUSED;
                        }
                    }
                }
                return IndicesShardStoresResponse.StoreStatus.Allocation.UNUSED;
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
