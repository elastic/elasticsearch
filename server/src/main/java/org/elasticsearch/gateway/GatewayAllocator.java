/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.ExistingShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShard;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodeGatewayBatchStartedShards;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.NodesGatewayBatchStartedShards;
import org.elasticsearch.gateway.TransportNodesBatchListGatewayStartedShards.ShardRequestInfo;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodesGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata.NodeBatchStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesBatchListShardStoreMetadata.NodesBatchStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodesStoreFilesMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.StoreFilesMetadata;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

public class GatewayAllocator implements ExistingShardsAllocator {

    public static final String ALLOCATOR_NAME = "gateway_allocator";

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections
        .newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeStoreFilesMetadata>> asyncFetchStore = ConcurrentCollections
        .newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    @Inject
    public GatewayAllocator(RerouteService rerouteService, NodeClient client) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(client);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(client);
    }

    @Override
    public void cleanCaches() {
        Releasables.close(asyncFetchStarted.values());
        asyncFetchStarted.clear();
        Releasables.close(asyncFetchStore.values());
        asyncFetchStore.clear();
    }

    // for tests
    protected GatewayAllocator() {
        this.rerouteService = null;
        this.primaryShardAllocator = null;
        this.replicaShardAllocator = null;
    }

    @Override
    public int getNumberOfInFlightFetches() {
        int count = 0;
        for (AsyncShardFetch<NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<NodeStoreFilesMetadata> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    @Override
    public void applyStartedShards(final List<ShardRouting> startedShards, final RoutingAllocation allocation) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }
    }

    @Override
    public void applyFailedShards(final List<FailedShard> failedShards, final RoutingAllocation allocation) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    @Override
    public void beforeAllocation(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
    }

    @Override
    public void afterPrimariesBeforeReplicas(RoutingAllocation allocation) {
        assert replicaShardAllocator != null;
        if (allocation.routingNodes().hasInactiveReplicas()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
    }

    @Override
    public void allocateUnassigned(
        ShardRouting shardRouting,
        final RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator, shardRouting, unassignedAllocationHandler);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler
    ) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    public void flushPendingShardFetchRequests() {
        // test case could be null
        if (primaryShardAllocator != null) {
            this.primaryShardAllocator.flushPendingFetchShardRequests();
        }
        if (replicaShardAllocator != null) {
            this.replicaShardAllocator.flushPendingFetchShardRequests();
        }
    }

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = nodes.getDataNodes()
                .stream()
                .map(node -> node.getValue().getEphemeralId())
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> new ParameterizedMessage(
                    "new nodes {} found, clearing primary async-fetch-store cache",
                    Sets.difference(newEphemeralIds, lastSeenEphemeralIds)
                )
            );
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(
        AsyncShardFetch<TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata> fetch,
        RoutingAllocation allocation
    ) {
        ShardRouting primary = allocation.routingNodes().activePrimary(fetch.shardId);
        if (primary != null) {
            fetch.clearCacheForNode(primary.currentNodeId());
        }
    }

    private boolean hasNewNodes(DiscoveryNodes nodes) {
        for (Map.Entry<String, DiscoveryNode> node : nodes.getDataNodes().entrySet()) {
            if (lastSeenEphemeralIds.contains(node.getValue().getEphemeralId()) == false) {
                return true;
            }
        }
        return false;
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(
            Logger logger,
            String type,
            ShardId shardId,
            String customDataPath,
            Lister<? extends BaseNodesResponse<T>, T> action
        ) {
            super(logger, type, shardId, customDataPath, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            rerouteService.reroute(
                "async_shard_fetch",
                Priority.HIGH,
                ActionListener.wrap(
                    r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                    e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)
                )
            );
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final NodeClient client;
        private final Object listenerMutex = new Object();
        // node batched shard requests
        private final Map<DiscoveryNode, List<ShardRequestInfo<NodeGatewayStartedShards>>> queuedRequests = new ConcurrentHashMap<>();
        // key: shardId + nodeId, value: listeners of this shard for specific node
        private final Map<String, List<ActionListener<BaseNodesResponse<NodeGatewayStartedShards>>>> queuedListeners =
            new ConcurrentHashMap<>();

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type

            AsyncShardFetch<NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> allocation.isBatchShardFetchMode()
                    ? new InternalAsyncFetch<>(
                        logger,
                        "batch_shard_started",
                        shardId,
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        batchLister
                    )
                    : new InternalAsyncFetch<>(
                        logger,
                        "shard_started",
                        shardId,
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        lister
                    )
            );
            AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }

        Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;

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

        Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> batchLister = (
            shardId,
            customDataPath,
            nodes,
            listener) -> {
            // group shards by node
            for (DiscoveryNode node : nodes) {
                List<ShardRequestInfo<NodeGatewayStartedShards>> nodeLevelRequests = queuedRequests.computeIfAbsent(
                    node,
                    n -> Collections.synchronizedList(new ArrayList<>())
                );
                nodeLevelRequests.add(new ShardRequestInfo<>(shardId, customDataPath, listener));

                synchronized (listenerMutex) {
                    // queue child listeners
                    List<ActionListener<BaseNodesResponse<NodeGatewayStartedShards>>> shardListeners = queuedListeners.computeIfAbsent(
                        shardId.toString().concat(node.getId()),
                        m -> Collections.synchronizedList(new ArrayList<>())
                    );
                    shardListeners.add(listener);
                }
            }
            if (logger.isTraceEnabled()) {
                for (DiscoveryNode node : nodes) {
                    logger.trace(
                        "Queued number of [{}] async fetch shard requests for node [{}]",
                        queuedRequests.get(node).size(),
                        node.getId()
                    );
                }
            }
        };

        @Override
        public void flushPendingFetchShardRequests() {
            Map<ShardId, String> shards = new HashMap<>();
            for (DiscoveryNode node : queuedRequests.keySet()) {
                for (var shardRequest : queuedRequests.get(node)) {
                    shards.put(shardRequest.shardId(), shardRequest.getCustomDataPath());
                }

                assert shards.isEmpty() == false;
                logger.debug("Batch sending number of {} shard async fetch request to node {}", shards.size(), node.getId());

                // send shards fetch request per node
                final var curNodeRequests = queuedRequests.get(node);
                client.executeLocally(
                    TransportNodesBatchListGatewayStartedShards.TYPE,
                    new TransportNodesBatchListGatewayStartedShards.Request(shards, new DiscoveryNode[] { node }),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(NodesGatewayBatchStartedShards nodesBatchStartedShards) {
                            if (nodesBatchStartedShards.failures().size() > 0) {
                                // single node, got failed node request then node response must be empty.
                                assert nodesBatchStartedShards.getNodes().size() == 0;
                                for (var request : curNodeRequests) {
                                    String listenersKey = request.shardId().toString().concat(node.getId());
                                    for (ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener : queuedListeners.get(
                                        listenersKey
                                    )) {
                                        listener.onResponse(
                                            new NodesGatewayStartedShards(
                                                nodesBatchStartedShards.getClusterName(),
                                                new ArrayList<>(), // empty response
                                                nodesBatchStartedShards.failures()
                                            )
                                        );
                                    }
                                    queuedListeners.get(listenersKey).clear();
                                }
                                return;
                            }

                            assert nodesBatchStartedShards.getNodes().size() == 1;
                            NodeGatewayBatchStartedShards startedShardResponse = nodesBatchStartedShards.getNodes().get(0);
                            List<NodeGatewayBatchStartedShard> startedShards = startedShardResponse.getStartedShards();
                            for (NodeGatewayBatchStartedShard shard : startedShards) {
                                // transfer to NodeGatewayStartedShards to bwc.
                                List<NodeGatewayStartedShards> listSingleStartedShards = new ArrayList<>(1);
                                listSingleStartedShards.add(
                                    new NodeGatewayStartedShards(
                                        shard.getNode(),
                                        shard.allocationId(),
                                        shard.primary(),
                                        shard.storeException()
                                    )
                                );

                                NodesGatewayStartedShards newNodesResponse = new NodesGatewayStartedShards(
                                    nodesBatchStartedShards.getClusterName(),
                                    listSingleStartedShards,
                                    nodesBatchStartedShards.failures()
                                );

                                // call child listener
                                String listenersKey = shard.getShardId().toString().concat(node.getId());
                                List<ActionListener<BaseNodesResponse<NodeGatewayStartedShards>>> listeners = queuedListeners.get(
                                    listenersKey
                                );
                                assert listeners.isEmpty() == false;
                                // there is no differences for single shard->node multiple requests
                                ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener = listeners.remove(
                                    listeners.size() - 1
                                );
                                listener.onResponse(newNodesResponse);

                                synchronized (listenerMutex) {
                                    // clean child listener
                                    if (listeners.isEmpty()) {
                                        queuedListeners.remove(listenersKey);
                                    }
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // clean requests and listeners of this node
                            for (var request : curNodeRequests) {
                                request.getListener().onFailure(e);
                                synchronized (listenerMutex) {
                                    queuedListeners.get(request.shardId().toString().concat(node.getId())).clear();
                                }
                            }
                        }
                    }
                );
                // clean queued node requests after sending done
                queuedRequests.remove(node);
            }
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;
        private final Object listenerMutex = new Object();
        // node batched shard requests
        private final Map<DiscoveryNode, List<ShardRequestInfo<NodeStoreFilesMetadata>>> queuedRequests = new ConcurrentHashMap<>();
        // key: shardId + nodeId, value: listeners of this shard for specific node
        private final Map<String, List<ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>>>> queuedListeners =
            new ConcurrentHashMap<>();

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitly type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            AsyncShardFetch<NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> allocation.isBatchShardFetchMode()
                    ? new InternalAsyncFetch<>(
                        logger,
                        "batch_shard_store",
                        shard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        batchLister
                    )
                    : new InternalAsyncFetch<>(
                        logger,
                        "shard_store",
                        shard.shardId(),
                        IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                        lister
                    )
            );
            AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> shardStores = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId())
            );
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }

        Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> lister = this::listStoreFilesMetadata;

        private void listStoreFilesMetadata(
            ShardId shardId,
            String customDataPath,
            DiscoveryNode[] nodes,
            ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener
        ) {
            var request = new TransportNodesListShardStoreMetadata.Request(shardId, customDataPath, nodes);
            client.executeLocally(
                TransportNodesListShardStoreMetadata.TYPE,
                request,
                ActionListener.wrap(listener::onResponse, listener::onFailure)
            );
        }

        Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> batchLister = (
            shardId,
            customDataPath,
            nodes,
            listener) -> {
            // group shards by node
            for (DiscoveryNode node : nodes) {
                var nodeLevelRequests = queuedRequests.computeIfAbsent(node, n -> Collections.synchronizedList(new ArrayList<>()));
                nodeLevelRequests.add(new ShardRequestInfo<>(shardId, customDataPath, listener));

                synchronized (listenerMutex) {
                    // queue child listeners
                    List<ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>>> shardListeners = queuedListeners.computeIfAbsent(
                        shardId.toString().concat(node.getId()),
                        m -> Collections.synchronizedList(new ArrayList<>())
                    );
                    shardListeners.add(listener);
                }
            }
            if (logger.isTraceEnabled()) {
                for (DiscoveryNode node : nodes) {
                    logger.trace(
                        "Queued number of [{}] async list store metadata requests for node [{}]",
                        queuedRequests.get(node).size(),
                        node.getId()
                    );
                }
            }
        };

        @Override
        public void flushPendingFetchShardRequests() {
            Map<ShardId, String> shards = new HashMap<>();
            for (DiscoveryNode node : queuedRequests.keySet()) {
                for (var shardRequest : queuedRequests.get(node)) {
                    shards.put(shardRequest.shardId(), shardRequest.getCustomDataPath());
                }

                assert shards.isEmpty() == false;
                logger.debug("Batch sending number of {} shards async fetch request to node {}", shards.size(), node.getId());

                // send shards fetch request per node
                final var curNodeRequests = queuedRequests.get(node);
                client.executeLocally(
                    TransportNodesBatchListShardStoreMetadata.TYPE,
                    new TransportNodesBatchListShardStoreMetadata.Request(shards, new DiscoveryNode[] { node }),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(NodesBatchStoreFilesMetadata nodesBatchStoreFilesMetadata) {
                            if (nodesBatchStoreFilesMetadata.failures().size() > 0) {
                                // single node, got failed node request then node response must be empty.
                                assert nodesBatchStoreFilesMetadata.getNodes().size() == 0;
                                for (var request : curNodeRequests) {
                                    String listenersKey = request.shardId().toString().concat(node.getId());
                                    for (ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener : queuedListeners.get(
                                        listenersKey
                                    )) {
                                        listener.onResponse(
                                            new NodesStoreFilesMetadata(
                                                nodesBatchStoreFilesMetadata.getClusterName(),
                                                new ArrayList<>(), // empty response
                                                nodesBatchStoreFilesMetadata.failures()
                                            )
                                        );
                                    }
                                    synchronized (listenerMutex) {
                                        queuedListeners.get(listenersKey).clear();
                                    }
                                }
                                return;
                            }

                            assert nodesBatchStoreFilesMetadata.getNodes().size() == 1;
                            NodeBatchStoreFilesMetadata storeFilesResponse = nodesBatchStoreFilesMetadata.getNodes().get(0);
                            List<StoreFilesMetadata> storeFiles = storeFilesResponse.storeFilesMetadataList();
                            for (StoreFilesMetadata shard : storeFiles) {
                                // transfer to NodeStoreFilesMetadata to bwc.
                                List<NodeStoreFilesMetadata> listStoreFiles = new ArrayList<>(1);
                                listStoreFiles.add(new NodeStoreFilesMetadata(node, shard));

                                NodesStoreFilesMetadata newNodesResponse = new NodesStoreFilesMetadata(
                                    nodesBatchStoreFilesMetadata.getClusterName(),
                                    listStoreFiles,
                                    nodesBatchStoreFilesMetadata.failures()
                                );

                                // call child listener
                                String listenersKey = shard.shardId().toString().concat(node.getId());
                                List<ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>>> listeners = queuedListeners.get(
                                    listenersKey
                                );
                                assert listeners.isEmpty() == false;
                                // there is no differences for single shard->node multiple requests
                                ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener = listeners.remove(listeners.size() - 1);
                                listener.onResponse(newNodesResponse);

                                synchronized (listenerMutex) {
                                    // clean child listener
                                    if (listeners.isEmpty()) {
                                        queuedListeners.remove(listenersKey);
                                    }
                                }
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // clean requests and listeners of this node
                            for (var request : curNodeRequests) {
                                request.getListener().onFailure(e);
                                synchronized (listenerMutex) {
                                    queuedListeners.get(request.shardId().toString().concat(node.getId())).clear();
                                }
                            }
                        }
                    }
                );
                // clean queued node requests after sending done
                queuedRequests.remove(node);
            }
        }
    }
}
