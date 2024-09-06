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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.Failure;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse.StoreStatus.AllocationStatus;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Transport action that reads the cluster state for shards with the requested criteria (see {@link ClusterHealthStatus}) of specific
 * indices and fetches store information from all the nodes using {@link TransportNodesListGatewayStartedShards}
 */
public class TransportIndicesShardStoresAction extends TransportMasterNodeReadAction<
    IndicesShardStoresRequest,
    IndicesShardStoresResponse> {

    public static final ActionType<IndicesShardStoresResponse> TYPE = new ActionType<>("indices:monitor/shard_stores");

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
            TYPE.name(),
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            IndicesShardStoresRequest::new,
            indexNameExpressionResolver,
            IndicesShardStoresResponse::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
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
        final DiscoveryNode[] nodes = state.nodes().getDataNodes().values().toArray(new DiscoveryNode[0]);
        final String[] concreteIndices = indexNameExpressionResolver.concreteIndexNames(state, request);
        final RoutingTable routingTable = state.routingTable();
        final Metadata metadata = state.metadata();
        logger.trace("using cluster state version [{}] to determine shards", state.version());
        assert task instanceof CancellableTask;
        new AsyncAction(
            (CancellableTask) task,
            concreteIndices,
            request.shardStatuses(),
            nodes,
            routingTable,
            metadata,
            request.maxConcurrentShardRequests(),
            listener
        ).run();
    }

    @Override
    protected ClusterBlockException checkBlock(IndicesShardStoresRequest request, ClusterState state) {
        return state.blocks()
            .indicesBlockedException(ClusterBlockLevel.METADATA_READ, indexNameExpressionResolver.concreteIndexNames(state, request));
    }

    // exposed for tests
    void listShardStores(
        TransportNodesListGatewayStartedShards.Request request,
        ActionListener<TransportNodesListGatewayStartedShards.NodesGatewayStartedShards> listener
    ) {
        // async fetch store infos from all the nodes for this one shard
        // NOTE: instead of fetching shard store info one by one from every node (nShards * nNodes requests)
        // we could fetch all shard store info from every node once (nNodes requests)
        // we have to implement a TransportNodesAction instead of using TransportNodesListGatewayStartedShards
        // for fetching shard stores info, that operates on a list of shards instead of a single shard

        client.executeLocally(TransportNodesListGatewayStartedShards.TYPE, request, listener);
    }

    private record ShardRequestContext(
        ShardId shardId,
        String customDataPath,
        ActionListener<TransportNodesListGatewayStartedShards.NodesGatewayStartedShards> listener
    ) {}

    private final class AsyncAction {
        private final CancellableTask task;
        private final DiscoveryNode[] nodes;
        private final String[] concreteIndices;
        private final RoutingTable routingTable;
        private final Metadata metadata;
        private final Map<String, Map<Integer, List<StoreStatus>>> indicesStatuses;
        private final int maxConcurrentShardRequests;
        private final Queue<Failure> failures;
        private final EnumSet<ClusterHealthStatus> requestedStatuses;
        private final RefCountingListener outerListener;

        private AsyncAction(
            CancellableTask task,
            String[] concreteIndices,
            EnumSet<ClusterHealthStatus> requestedStatuses,
            DiscoveryNode[] nodes,
            RoutingTable routingTable,
            Metadata metadata,
            int maxConcurrentShardRequests,
            ActionListener<IndicesShardStoresResponse> listener
        ) {
            this.task = task;
            this.nodes = nodes;
            this.concreteIndices = concreteIndices;
            this.routingTable = routingTable;
            this.metadata = metadata;
            this.requestedStatuses = requestedStatuses;

            this.indicesStatuses = Collections.synchronizedMap(Maps.newHashMapWithExpectedSize(concreteIndices.length));
            this.maxConcurrentShardRequests = maxConcurrentShardRequests;
            this.failures = new ConcurrentLinkedQueue<>();
            this.outerListener = new RefCountingListener(1, listener.map(ignored -> {
                task.ensureNotCancelled();
                return new IndicesShardStoresResponse(Map.copyOf(indicesStatuses), List.copyOf(failures));
            }));
        }

        private boolean isFailing() {
            return outerListener.isFailing() || task.isCancelled();
        }

        void run() {
            ThrottledIterator.run(
                Iterators.flatMap(Iterators.forArray(concreteIndices), this::getIndexIterator),
                this::doShardRequest,
                maxConcurrentShardRequests,
                () -> {},
                outerListener::close
            );
        }

        private Iterator<ShardRequestContext> getIndexIterator(String indexName) {
            if (isFailing()) {
                return Collections.emptyIterator();
            }

            final var indexRoutingTable = routingTable.index(indexName);
            if (indexRoutingTable == null) {
                return Collections.emptyIterator();
            }

            return new IndexRequestContext(indexRoutingTable).getShardRequestContexts();
        }

        private void doShardRequest(Releasable ref, ShardRequestContext shardRequestContext) {
            ActionListener.run(ActionListener.releaseAfter(shardRequestContext.listener(), ref), l -> {
                if (isFailing()) {
                    l.onResponse(null);
                } else {
                    listShardStores(
                        new TransportNodesListGatewayStartedShards.Request(
                            shardRequestContext.shardId(),
                            shardRequestContext.customDataPath(),
                            nodes
                        ),
                        l
                    );
                }
            });
        }

        private class IndexRequestContext {
            private final IndexRoutingTable indexRoutingTable;
            private final Map<Integer, List<StoreStatus>> indexResults;

            IndexRequestContext(IndexRoutingTable indexRoutingTable) {
                this.indexRoutingTable = indexRoutingTable;
                this.indexResults = Collections.synchronizedMap(Maps.newHashMapWithExpectedSize(indexRoutingTable.size()));
            }

            Iterator<ShardRequestContext> getShardRequestContexts() {
                try (var shardListeners = new RefCountingListener(1, outerListener.acquire(ignored -> putResults()))) {
                    final var customDataPath = IndexMetadata.INDEX_DATA_PATH_SETTING.get(
                        metadata.index(indexRoutingTable.getIndex()).getSettings()
                    );
                    final var shardRequestContexts = new ArrayList<ShardRequestContext>(indexRoutingTable.size());
                    for (int shardNum = 0; shardNum < indexRoutingTable.size(); shardNum++) {
                        final var indexShardRoutingTable = indexRoutingTable.shard(shardNum);
                        final var clusterShardHealth = new ClusterShardHealth(shardNum, indexShardRoutingTable);
                        if (requestedStatuses.contains(clusterShardHealth.getStatus())) {
                            shardRequestContexts.add(
                                new ShardRequestContext(
                                    indexShardRoutingTable.shardId(),
                                    customDataPath,
                                    shardListeners.acquire(fetchResponse -> handleFetchResponse(indexShardRoutingTable, fetchResponse))
                                )
                            );
                        }
                    }
                    return shardRequestContexts.iterator();
                }
            }

            private void handleFetchResponse(
                IndexShardRoutingTable indexShardRoutingTable,
                TransportNodesListGatewayStartedShards.NodesGatewayStartedShards fetchResponse
            ) {
                if (isFailing()) {
                    return;
                }

                final var shardId = indexShardRoutingTable.shardId();

                for (FailedNodeException failure : fetchResponse.failures()) {
                    failures.add(new Failure(failure.nodeId(), shardId.getIndexName(), shardId.getId(), failure.getCause()));
                }

                final var shardStores = fetchResponse.getNodes()
                    .stream()
                    .filter(IndexRequestContext::shardExistsInNode)
                    .map(
                        nodeResponse -> new StoreStatus(
                            nodeResponse.getNode(),
                            nodeResponse.allocationId(),
                            getAllocationStatus(indexShardRoutingTable, nodeResponse.getNode()),
                            nodeResponse.storeException()
                        )
                    )
                    .sorted()
                    .toList();

                indexResults.put(shardId.getId(), shardStores);
            }

            private void putResults() {
                if (isFailing() == false && indexResults.isEmpty() == false) {
                    indicesStatuses.put(indexRoutingTable.getIndex().getName(), Map.copyOf(indexResults));
                }
            }

            /**
             * A shard exists/existed in a node only if shard state file exists in the node
             */
            private static boolean shardExistsInNode(final NodeGatewayStartedShards response) {
                return response.storeException() != null || response.allocationId() != null;
            }

            private static AllocationStatus getAllocationStatus(IndexShardRoutingTable indexShardRoutingTable, DiscoveryNode node) {
                for (final var shardRouting : indexShardRoutingTable.assignedShards()) {
                    if (node.getId().equals(shardRouting.currentNodeId())) {
                        return shardRouting.primary() ? AllocationStatus.PRIMARY : AllocationStatus.REPLICA;
                    }
                }
                return AllocationStatus.UNUSED;
            }
        }
    }
}
