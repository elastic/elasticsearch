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

package org.elasticsearch.gateway;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class GatewayAllocator {

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>>
        asyncFetchStarted = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData>>
        asyncFetchStore = ConcurrentCollections.newConcurrentMap();
    private Set<String> lastSeenEphemeralIds = Collections.emptySet();

    @Inject
    public GatewayAllocator(RerouteService rerouteService,
                            TransportNodesListGatewayStartedShards startedAction,
                            TransportNodesListShardStoreMetaData storeAction) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(startedAction);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(storeAction);
    }

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

    public int getNumberOfInFlightFetch() {
        int count = 0;
        for (AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch : asyncFetchStore.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        return count;
    }

    public void applyStartedShards(final RoutingAllocation allocation, final List<ShardRouting> startedShards) {
        for (ShardRouting startedShard : startedShards) {
            Releasables.close(asyncFetchStarted.remove(startedShard.shardId()));
            Releasables.close(asyncFetchStore.remove(startedShard.shardId()));
        }
    }

    public void applyFailedShards(final RoutingAllocation allocation, final List<FailedShard> failedShards) {
        for (FailedShard failedShard : failedShards) {
            Releasables.close(asyncFetchStarted.remove(failedShard.getRoutingEntry().shardId()));
            Releasables.close(asyncFetchStore.remove(failedShard.getRoutingEntry().shardId()));
        }
    }

    public void allocateUnassigned(final RoutingAllocation allocation) {
        assert primaryShardAllocator != null;
        assert replicaShardAllocator != null;
        ensureAsyncFetchStorePrimaryRecency(allocation);
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(RoutingAllocation allocation,
                                                   PrimaryShardAllocator primaryShardAllocator,
                                                   ReplicaShardAllocator replicaShardAllocator) {
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        unassigned.sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        primaryShardAllocator.allocateUnassigned(allocation);
        if (allocation.routingNodes().hasInactiveShards()) {
            // cancel existing recoveries if we have a better match
            replicaShardAllocator.processExistingRecoveries(allocation);
        }
        replicaShardAllocator.allocateUnassigned(allocation);
    }

    /**
     * Computes and returns the design for allocating a single unassigned shard.  If called on an assigned shard,
     * {@link AllocateUnassignedDecision#NOT_TAKEN} is returned.
     */
    public AllocateUnassignedDecision decideUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
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
            final Set<String> newEphemeralIds = StreamSupport.stream(nodes.getDataNodes().spliterator(), false)
                .map(node -> node.value.getEphemeralId()).collect(Collectors.toSet());
            // Invalidate the cache if a data node has been added to the cluster. This ensures that we do not cancel a recovery if a node
            // drops out, we fetch the shard data, then some indexing happens and then the node rejoins the cluster again. There are other
            // ways we could decide to cancel a recovery based on stale data (e.g. changing allocation filters or a primary failure) but
            // making the wrong decision here is not catastrophic so we only need to cover the common case.
            logger.trace(() -> new ParameterizedMessage(
                "new nodes {} found, clearing primary async-fetch-store cache", Sets.difference(newEphemeralIds, lastSeenEphemeralIds)));
            asyncFetchStore.values().forEach(fetch -> clearCacheForPrimary(fetch, allocation));
            // recalc to also (lazily) clear out old nodes.
            this.lastSeenEphemeralIds = newEphemeralIds;
        }
    }

    private static void clearCacheForPrimary(AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch,
                                             RoutingAllocation allocation) {
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

        InternalAsyncFetch(Logger logger, String type, ShardId shardId, String customDataPath,
                           Lister<? extends BaseNodesResponse<T>, T> action) {
            super(logger, type, shardId, customDataPath, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            assert rerouteService != null;
            rerouteService.reroute("async_shard_fetch", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)));
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final TransportNodesListGatewayStartedShards startedAction;

        InternalPrimaryShardAllocator(TransportNodesListGatewayStartedShards startedAction) {
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>
                                                                        fetchData(ShardRouting shard, RoutingAllocation allocation) {
            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch =
                asyncFetchStarted.computeIfAbsent(shard.shardId(),
                    shardId -> new InternalAsyncFetch<>(logger, "shard_started", shardId,
                        IndexMetaData.INDEX_DATA_PATH_SETTING.get(allocation.metaData().index(shard.index()).getSettings()),
                        startedAction));
            AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> shardState =
                    fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final TransportNodesListShardStoreMetaData storeAction;

        InternalReplicaShardAllocator(TransportNodesListShardStoreMetaData storeAction) {
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData>
                                                                        fetchData(ShardRouting shard, RoutingAllocation allocation) {
            AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch =
                asyncFetchStore.computeIfAbsent(shard.shardId(),
                    shardId -> new InternalAsyncFetch<>(logger, "shard_store", shard.shardId(),
                        IndexMetaData.INDEX_DATA_PATH_SETTING.get(allocation.metaData().index(shard.index()).getSettings()), storeAction));
            AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> shardStores =
                    fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));
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
