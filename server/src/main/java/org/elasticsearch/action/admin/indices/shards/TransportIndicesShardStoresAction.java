/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.indices.shards;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific
 * indices and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardStoresAction
        extends TransportMasterNodeReadAction<IndicesShardStoresRequest, IndicesShardStoresResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndicesShardStoresAction.class);

    private final NodeClient client;

    @Inject
    public TransportIndicesShardStoresAction(TransportService transportService, ClusterService clusterService,
                                             ThreadPool threadPool, ActionFilters actionFilters,
                                             IndexNameExpressionResolver indexNameExpressionResolver, NodeClient client) {
        super(IndicesShardStoresAction.NAME, transportService, clusterService, threadPool, actionFilters,
            IndicesShardStoresRequest::new, indexNameExpressionResolver, IndicesShardStoresResponse::new, ThreadPool.Names.SAME);
        this.client = client;
    }

    @Override
    protected void masterOperation(Task task, IndicesShardStoresRequest request, ClusterState state,
                                   ActionListener<IndicesShardStoresResponse> listener) {
        final RoutingTable routingTables = state.routingTable();
        final RoutingNodes routingNodes = state.getRoutingNodes();
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        final Set<Tuple<ShardId, String>> shardsToFetch = new HashSet<>();

        logger.trace("using cluster state version [{}] to determine shards", state.version());
        // collect relevant shard ids of the requested indices for fetching store infos
        for (String index : concreteIndices) {
            IndexRoutingTable indexShardRoutingTables = routingTables.index(index);
            if (indexShardRoutingTables == null) {
                continue;
            }
            final String customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(state.metadata().index(index).getSettings());
            for (IndexShardRoutingTable routing : indexShardRoutingTables) {
                final int shardId = routing.shardId().id();
                ClusterShardHealth shardHealth = new ClusterShardHealth(shardId, routing);
                if (request.shardStatuses().contains(shardHealth.getStatus())) {
                    shardsToFetch.add(Tuple.tuple(routing.shardId(), customDataPath));
                }
            }
        }

        // async fetch store infos from all the nodes
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard
        new AsyncShardStoresInfoFetches(state.nodes(), routingNodes, shardsToFetch, listener).start();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardStoresRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.METADATA_READ,
            indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    private class AsyncShardStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingNodes routingNodes;
        private final Set<Tuple<ShardId, String>> shards;
        private final ActionListener<IndicesShardStoresResponse> listener;
        private CountDown expectedOps;
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardStoresInfoFetches(DiscoveryNodes nodes, RoutingNodes routingNodes, Set<Tuple<ShardId, String>> shards,
                                    ActionListener<IndicesShardStoresResponse> listener) {
            this.nodes = nodes;
            this.routingNodes = routingNodes;
            this.shards = shards;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
            this.expectedOps = new CountDown(shards.size());
        }

        void start() {
            if (shards.isEmpty()) {
                listener.onResponse(new IndicesShardStoresResponse());
            } else {
                // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
                Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;
                for (Tuple<ShardId, String> shard : shards) {
                    InternalAsyncFetch fetch = new InternalAsyncFetch(logger, "shard_stores", shard.v1(), shard.v2(), lister);
                    fetch.fetchData(nodes, Collections.<String>emptySet());
                }
            }
        }

        private void listStartedShards(ShardId shardId, String customDataPath, DiscoveryNode[] nodes,
                                       ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes);
            client.executeLocally(TransportNodesListGatewayStartedShards.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }

        private class InternalAsyncFetch extends AsyncShardFetch<NodeGatewayStartedShards> {

            InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath,
                               Lister<? extends BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> action) {
                super(logger, type, shardId, customDataPath, action);
            }

            @Override
            protected synchronized void processAsyncFetch(List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures,
                                                          long fetchingRound) {
                fetchResponses.add(new Response(shardId, responses, failures));
                if (expectedOps.countDown()) {
                    finish();
                }
            }

            void finish() {
                ImmutableOpenMap.Builder<String, ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>>>
                    indicesStoreStatusesBuilder = ImmutableOpenMap.builder();

                java.util.List<IndicesShardStoresResponse.Failure> failureBuilder = new ArrayList<>();
                for (Response fetchResponse : fetchResponses) {
                    ImmutableOpenIntMap<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexStoreStatuses =
                        indicesStoreStatusesBuilder.get(fetchResponse.shardId.getIndexName());
                    final ImmutableOpenIntMap.Builder<java.util.List<IndicesShardStoresResponse.StoreStatus>> indexShardsBuilder;
                    if (indexStoreStatuses == null) {
                        indexShardsBuilder = ImmutableOpenIntMap.builder();
                    } else {
                        indexShardsBuilder = ImmutableOpenIntMap.builder(indexStoreStatuses);
                    }
                    java.util.List<IndicesShardStoresResponse.StoreStatus> storeStatuses = indexShardsBuilder
                        .get(fetchResponse.shardId.id());
                    if (storeStatuses == null) {
                        storeStatuses = new ArrayList<>();
                    }
                    for (NodeGatewayStartedShards response : fetchResponse.responses) {
                        if (shardExistsInNode(response)) {
                            IndicesShardStoresResponse.StoreStatus.AllocationStatus allocationStatus = getAllocationStatus(
                                fetchResponse.shardId.getIndexName(), fetchResponse.shardId.id(), response.getNode());
                            storeStatuses.add(new IndicesShardStoresResponse.StoreStatus(response.getNode(), response.allocationId(),
                                allocationStatus, response.storeException()));
                        }
                    }
                    CollectionUtil.timSort(storeStatuses);
                    indexShardsBuilder.put(fetchResponse.shardId.id(), storeStatuses);
                    indicesStoreStatusesBuilder.put(fetchResponse.shardId.getIndexName(), indexShardsBuilder.build());
                    for (FailedNodeException failure : fetchResponse.failures) {
                        failureBuilder.add(new IndicesShardStoresResponse.Failure(failure.nodeId(), fetchResponse.shardId.getIndexName(),
                            fetchResponse.shardId.id(), failure.getCause()));
                    }
                }
                listener.onResponse(new IndicesShardStoresResponse(indicesStoreStatusesBuilder.build(),
                    Collections.unmodifiableList(failureBuilder)));
            }

            private IndicesShardStoresResponse.StoreStatus.AllocationStatus getAllocationStatus(String index, int shardID,
                                                                                                DiscoveryNode node) {
                for (ShardRouting shardRouting : routingNodes.node(node.getId())) {
                    ShardId shardId = shardRouting.shardId();
                    if (shardId.id() == shardID && shardId.getIndexName().equals(index)) {
                        if (shardRouting.primary()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.PRIMARY;
                        } else if (shardRouting.assignedToNode()) {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.REPLICA;
                        } else {
                            return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
                        }
                    }
                }
                return IndicesShardStoresResponse.StoreStatus.AllocationStatus.UNUSED;
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             */
            private boolean shardExistsInNode(final NodeGatewayStartedShards response) {
                return response.storeException() != null || response.allocationId() != null;
            }

            @Override
            protected void reroute(ShardId shardId, String reason) {
                // no-op
            }

            public class Response {
                private final ShardId shardId;
                private final List<NodeGatewayStartedShards> responses;
                private final List<FailedNodeException> failures;

                Response(ShardId shardId, List<NodeGatewayStartedShards> responses, List<FailedNodeException> failures) {
                    this.shardId = shardId;
                    this.responses = responses;
                    this.failures = failures;
                }
            }
        }
    }
}
