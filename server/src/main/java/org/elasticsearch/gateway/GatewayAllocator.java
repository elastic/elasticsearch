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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.internal.node.NodeClient;
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
import org.elasticsearch.core.Releasables;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetadata.NodeStoreFilesMetadata;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.difference;
import static org.elasticsearch.core.Strings.format;

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
            Releasables.close(asyncFetchStarted.remove(failedShard.routingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.routingEntry().shardId()));
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

    /**
     * Clear the fetched data for the primary to ensure we do not cancel recoveries based on excessively stale data.
     */
    private void ensureAsyncFetchStorePrimaryRecency(RoutingAllocation allocation) {
        DiscoveryNodes nodes = allocation.nodes();
        if (hasNewNodes(nodes)) {
            final Set<String> newEphemeralIds = nodes.getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getEphemeralId)
                .collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(
                () -> format(
                    "new nodes %s found, clearing primary async-fetch-store cache",
                    difference(newEphemeralIds, lastSeenEphemeralIds)
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

    abstract class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath, int expectedSize) {
            super(logger, type, shardId, customDataPath, expectedSize);
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
                    e -> logger.debug(() -> format("%s scheduled reroute failed for %s", shardId, reason), e)
                )
            );
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final NodeClient client;

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitly type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            AsyncShardFetch<NodeGatewayStartedShards> fetch = asyncFetchStarted.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_started",
                    shardId,
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    allocation.routingNodes().size()
                ) {
                    @Override
                    protected void list(
                        ShardId shardId,
                        String customDataPath,
                        DiscoveryNode[] nodes,
                        ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener
                    ) {
                        client.executeLocally(
                            TransportNodesListGatewayStartedShards.TYPE,
                            new TransportNodesListGatewayStartedShards.Request(shardId, customDataPath, nodes),
                            ActionListener.wrap(listener)
                        );
                    }
                }
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

    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetadata> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            AsyncShardFetch<NodeStoreFilesMetadata> fetch = asyncFetchStore.computeIfAbsent(
                shard.shardId(),
                shardId -> new InternalAsyncFetch<>(
                    logger,
                    "shard_store",
                    shard.shardId(),
                    IndexMetadata.INDEX_DATA_PATH_SETTING.get(allocation.metadata().index(shard.index()).getSettings()),
                    allocation.routingNodes().size()
                ) {
                    @Override
                    protected void list(
                        ShardId shardId,
                        String customDataPath,
                        DiscoveryNode[] nodes,
                        ActionListener<BaseNodesResponse<NodeStoreFilesMetadata>> listener
                    ) {
                        client.executeLocally(
                            TransportNodesListShardStoreMetadata.TYPE,
                            new TransportNodesListShardStoreMetadata.Request(shardId, customDataPath, nodes),
                            ActionListener.wrap(listener)
                        );
                    }
                }
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
    }
}
