/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
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
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.ShardRequestInfo;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodesGroupedGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGroupedGatewayStartedShards;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodesGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;

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
        UnassignedAllocationHandler unassignedAllocationHandler,
        boolean flushAsyncShardFetching
    ) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator,
            shardRouting, unassignedAllocationHandler, flushAsyncShardFetching);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(
        RoutingAllocation allocation,
        PrimaryShardAllocator primaryShardAllocator,
        ReplicaShardAllocator replicaShardAllocator,
        ShardRouting shardRouting,
        ExistingShardsAllocator.UnassignedAllocationHandler unassignedAllocationHandler,
        boolean flushAsyncShardFetching
    ) {
        assert shardRouting.unassigned();
        if (shardRouting.primary()) {
            primaryShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler, flushAsyncShardFetching);
        } else {
            replicaShardAllocator.allocateUnassigned(shardRouting, allocation, unassignedAllocationHandler, flushAsyncShardFetching);
        }
    }

    @Override
    public AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        assert unassignedShard.unassigned();
        assert routingAllocation.debugDecision();
        if (unassignedShard.primary()) {
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, true, logger);
        } else {
            assert replicaShardAllocator != null;
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, true, logger);
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
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getDataNodes()) {
            if (lastSeenEphemeralIds.contains(node.value.getEphemeralId()) == false) {
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

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        // node grouped shard requests
        private final Map<DiscoveryNode, List<ShardRequestInfo>> queuedRequests = new ConcurrentHashMap<>();
        // key: shardId + nodeId, value: listeners of this shard for specific node
        private final Map<String, List<ActionListener<NodesGatewayStartedShards>>> queuedListeners = new ConcurrentHashMap<>();

        Lister<NodesGatewayStartedShards, NodeGatewayStartedShards> lister =
            new Lister<>() {
                @Override
                public void list(ShardId shardId, String customDataPath, DiscoveryNode[] nodes,
                                 ActionListener<NodesGatewayStartedShards> listener) {
                    // group shards by node
                    for (DiscoveryNode node : nodes) {
                        List<ShardRequestInfo> nodeLevelRequests = queuedRequests.computeIfAbsent(node,
                            n -> Collections.synchronizedList(new ArrayList<>()));
                        nodeLevelRequests.add(new ShardRequestInfo(shardId, customDataPath, listener));

                        // queue child listeners
                        List<ActionListener<NodesGatewayStartedShards>> shardListeners =
                            queuedListeners.computeIfAbsent(shardId.toString().concat(node.getId()),
                                m -> Collections.synchronizedList(new ArrayList<>()));
                        shardListeners.add(listener);
                    }
                }

                @Override
                public void flush() {
                    Map<ShardId, String> shards = new HashMap<>();
                    for (DiscoveryNode node: queuedRequests.keySet()) {
                        for (ShardRequestInfo shardRequest : queuedRequests.get(node)) {
                            shards.put(shardRequest.shardId(), shardRequest.getCustomDataPath());
                            if (node.getVersion().before(Version.V_7_16_0)) {
                                // bwc, send single shard to node
                                ActionListener<NodesGroupedGatewayStartedShards> listener = new ActionListener<>() {
                                    @Override
                                    public void onResponse(TransportNodesListGatewayStartedShards.NodesGroupedGatewayStartedShards
                                                               nodesGroupedStartedShards) {
                                        assert nodesGroupedStartedShards.getNodes().size() == 1;
                                        NodeGroupedGatewayStartedShards startedShardResponse = nodesGroupedStartedShards.getNodes().get(0);

                                        List<NodeGatewayStartedShards> startedShards = startedShardResponse.getStartedShards();
                                        assert startedShards.size() == 1;

                                        NodesGatewayStartedShards newNodesResponse = new NodesGatewayStartedShards(
                                            nodesGroupedStartedShards.getClusterName(),
                                            startedShards,
                                            nodesGroupedStartedShards.failures());
                                        shardRequest.getListener().onResponse(newNodesResponse);
                                    }

                                    @Override
                                    public void onFailure(Exception e) {
                                        shardRequest.getListener().onFailure(e);
                                    }
                                };
                                client.executeLocally(
                                    TransportNodesListGatewayStartedShards.TYPE,
                                    new TransportNodesListGatewayStartedShards.Request(shards, new DiscoveryNode[]{node}),
                                    listener);
                                shards.remove(shardRequest.shardId());
                            }
                        }

                        assert shards.isEmpty() == false;
                        logger.debug("Batch sending number of {} shard async fetch request to node {}", shards.size(), node.getId());

                        if (node.getVersion().onOrAfter(Version.V_7_16_0)) {
                            // send shard fetch request per node
                            final List<ShardRequestInfo> curNodeRequests = queuedRequests.get(node);
                            ActionListener<NodesGroupedGatewayStartedShards> listener = new ActionListener<>() {
                                @Override
                                public void onResponse(NodesGroupedGatewayStartedShards nodesGroupedStartedShards) {
                                    if (nodesGroupedStartedShards.failures().size() > 0) {
                                        assert nodesGroupedStartedShards.getNodes().size() == 0;
                                        for (ShardRequestInfo request : curNodeRequests) {
                                            String listenersKey = request.shardId().toString().concat(node.getId());
                                            for (ActionListener<NodesGatewayStartedShards> listener : queuedListeners.get(listenersKey)) {
                                                listener.onResponse(new NodesGatewayStartedShards(
                                                    nodesGroupedStartedShards.getClusterName(),
                                                    new ArrayList<>(),
                                                    nodesGroupedStartedShards.failures()));
                                            }
                                            queuedListeners.get(listenersKey).clear();
                                        }
                                        return;
                                    }

                                    assert nodesGroupedStartedShards.getNodes().size() == 1;
                                    NodeGroupedGatewayStartedShards startedShardResponse = nodesGroupedStartedShards.getNodes().get(0);
                                    List<NodeGatewayStartedShards> startedShards = startedShardResponse.getStartedShards();
                                    for (NodeGatewayStartedShards startedShard : startedShards) {
                                        // transfer to single shard -> nodes (single node) response
                                        List<NodeGatewayStartedShards> listSingleStartedShards = new ArrayList<>();
                                        listSingleStartedShards.add(startedShard);
                                        NodesGatewayStartedShards newNodesResponse = new NodesGatewayStartedShards(
                                            nodesGroupedStartedShards.getClusterName(),
                                            listSingleStartedShards,
                                            nodesGroupedStartedShards.failures());

                                        // call child listener
                                        String listenersKey = startedShard.getShardId().toString().concat(node.getId());
                                        List<ActionListener<NodesGatewayStartedShards>> listeners = queuedListeners.get(listenersKey);
                                        assert listeners.isEmpty() == false;
                                        ActionListener<NodesGatewayStartedShards> listener = listeners.remove(listeners.size() - 1);
                                        listener.onResponse(newNodesResponse);

                                        // clean child listener
                                        if (listeners.isEmpty()) {
                                            queuedListeners.remove(listenersKey);
                                        }
                                    }
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    // clean requests and listeners of this node
                                    for (ShardRequestInfo request : curNodeRequests) {
                                        request.getListener().onFailure(e);
                                        queuedListeners.get(request.shardId().toString().concat(node.getId())).clear();
                                    }
                                }
                            };

                            client.executeLocally(
                                TransportNodesListGatewayStartedShards.TYPE,
                                new TransportNodesListGatewayStartedShards.Request(shards, new DiscoveryNode[]{node}),
                                listener);
                            // clean queued node requests after sending done
                            queuedRequests.remove(node);
                        }
                    }
                }
            };

        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard,
                                                                                  RoutingAllocation allocation,
                                                                                  boolean flushAsyncShardFetching) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type

            AsyncShardFetch<NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_started",
                    shardId,
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    lister
                )
            );
            AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState = fetch.fetchData(
                allocation.nodes(),
                allocation.getIgnoreNodes(shard.shardId()),
                flushAsyncShardFetching
            );

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitly type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            Lister<BaseNodesResponse<NodeStoreFilesMetadata>, NodeStoreFilesMetadata> lister = this::listStoreFilesMetadata;
            AsyncShardFetch<NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
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

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }
}
