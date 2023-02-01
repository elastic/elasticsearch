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
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.Failure;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus.AllocationStatus;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.internal.node.NodeClient;
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
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.AsyncShardFetch;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific
 * indices and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardStoresAction extends TransportMasterNodeReadAction<
    IndicesShardStoresRequest,
    IndicesShardStoresResponse> {

    private static final Logger logger = LogManager.getLogger(TransportIndicesShardStoresAction.class);

    private final NodeClient client;

    @Inject
    public TransportIndicesShardStoresAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        IndexNameExpressionResolver indexNameExpressionResolver,
        NodeClient client
    ) {
        super(
            IndicesShardStoresAction.NAME,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesShardStoresRequest::new,
            indexNameExpressionResolver,
            IndicesShardStoresResponse::new,
            ThreadPool.Names.SAME
        );
        this.client = client;
    }

    @Override
    protected void masterOperation(
        Task task,
        IndicesShardStoresRequest request,
        ClusterState state,
        ActionListener<IndicesShardStoresResponse> listener
    ) {
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
            for (int i = 0; i < indexShardRoutingTables.size(); i++) {
                IndexShardRoutingTable routing = indexShardRoutingTables.shard(i);
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
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    private class AsyncShardStoresInfoFetches {
        private final DiscoveryNodes nodes;
        private final RoutingNodes routingNodes;
        private final Set<Tuple<ShardId, String>> shards;
        private final ActionListener<IndicesShardStoresResponse> listener;
        private final RefCountingRunnable refs = new RefCountingRunnable(this::finish);
        private final Queue<InternalAsyncFetch.Response> fetchResponses;

        AsyncShardStoresInfoFetches(
            DiscoveryNodes nodes,
            RoutingNodes routingNodes,
            Set<Tuple<ShardId, String>> shards,
            ActionListener<IndicesShardStoresResponse> listener
        ) {
            this.nodes = nodes;
            this.routingNodes = routingNodes;
            this.shards = shards;
            this.listener = listener;
            this.fetchResponses = new ConcurrentLinkedQueue<>();
        }

        void start() {
            try {
                for (Tuple<ShardId, String> shard : shards) {
                    new InternalAsyncFetch(logger, "shard_stores", shard.v1(), shard.v2()).fetchData(nodes, Collections.emptySet());
                }
            } finally {
                refs.close();
            }
        }

        private void listStartedShards(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener
        ) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes);
            client.executeLocally(
                TransportNodesListGatewayStartedShards.TYPE,
                request,
                ActionListener.wrap(listener::onResponse, listener::onFailure)
            );
        }

        private class InternalAsyncFetch extends AsyncShardFetch<NodeGatewayStartedShards> {

            private final Releasable ref = refs.acquire();

            InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath) {
                super(logger, type, shardId, customDataPath);
            }

            @Override
            protected synchronized void processAsyncFetch(
                List<NodeGatewayStartedShards> responses,
                List<FailedNodeException> failures,
                long fetchingRound
            ) {
                fetchResponses.add(new Response(shardId, responses, failures));
                ref.close();
            }

            @Override
            protected void list(
                ShardId shardId,
                String customDataPath,
                DiscoveryNode[] nodes,
                ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener
            ) {
                listStartedShards(shardId, customDataPath, nodes, listener);
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

        void finish() {
            Map<String, Map<Integer, List<StoreStatus>>> indicesStatuses = new HashMap<>();
            List<Failure> failures = new ArrayList<>();
            for (InternalAsyncFetch.Response fetchResponse : fetchResponses) {
                var indexName = fetchResponse.shardId.getIndexName();
                var shardId = fetchResponse.shardId.id();
                var indexStatuses = indicesStatuses.computeIfAbsent(indexName, k -> new HashMap<>());
                var storeStatuses = indexStatuses.computeIfAbsent(shardId, k -> new ArrayList<>());

                for (NodeGatewayStartedShards r : fetchResponse.responses) {
                    if (shardExistsInNode(r)) {
                        var allocationStatus = getAllocationStatus(indexName, shardId, r.getNode());
                        storeStatuses.add(new StoreStatus(r.getNode(), r.allocationId(), allocationStatus, r.storeException()));
                    }
                }

                for (FailedNodeException failure : fetchResponse.failures) {
                    failures.add(new Failure(failure.nodeId(), indexName, shardId, failure.getCause()));
                }
            }
            // make the status structure immutable
            indicesStatuses.replaceAll((k, v) -> {
                v.replaceAll((s, l) -> {
                    CollectionUtil.timSort(l);
                    return List.copyOf(l);
                });
                return Map.copyOf(v);
            });
            listener.onResponse(new IndicesShardStoresResponse(Map.copyOf(indicesStatuses), List.copyOf(failures)));
        }

        private AllocationStatus getAllocationStatus(String index, int shardID, DiscoveryNode node) {
            for (ShardRouting shardRouting : routingNodes.node(node.getId())) {
                ShardId shardId = shardRouting.shardId();
                if (shardId.id() == shardID && shardId.getIndexName().equals(index)) {
                    if (shardRouting.primary()) {
                        return AllocationStatus.PRIMARY;
                    } else if (shardRouting.assignedToNode()) {
                        return AllocationStatus.REPLICA;
                    } else {
                        return AllocationStatus.UNUSED;
                    }
                }
            }
            return AllocationStatus.UNUSED;
        }

        /**
         * A shard exists/existed in a node only if shard state file exists in the node
         */
        private static boolean shardExistsInNode(final NodeGatewayStartedShards response) {
            return response.storeException() != null || response.allocationId() != null;
        }
    }
}
