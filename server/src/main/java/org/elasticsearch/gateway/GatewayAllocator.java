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

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingService;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class GatewayAllocator extends AbstractComponent {

    private final RoutingService routingService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards>> asyncFetchStarted = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData>> asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    @Inject
    public GatewayAllocator(Settings settings, ClusterService clusterService, RoutingService routingService,
                            TransportNodesListGatewayStartedShards startedAction, TransportNodesListShardStoreMetaData storeAction) {
        super(settings);
        this.routingService = routingService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(settings, startedAction);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(settings, storeAction);
        clusterService.addStateApplier(event -> {
            boolean cleanCache = false;
            DiscoveryNode localNode = event.state().nodes().getLocalNode();
            if (localNode != null) {
                if (localNode.isMasterNode() && event.localNodeMaster() == false) {
                    cleanCache = true;
                }
            } else {
                cleanCache = true;
            }
            if (cleanCache) {
                Releasables.close(asyncFetchStarted.values());
                asyncFetchStarted.clear();
                Releasables.close(asyncFetchStore.values());
                asyncFetchStore.clear();
            }
        });
    }

    // for tests
    protected GatewayAllocator(Settings settings) {
        super(settings);
        this.routingService = null;
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
        innerAllocatedUnassigned(allocation, primaryShardAllocator, replicaShardAllocator);
    }

    // allow for testing infra to change shard allocators implementation
    protected static void innerAllocatedUnassigned(RoutingAllocation allocation,
                                                   PrimaryShardAllocator primaryShardAllocator,
                                                   ReplicaShardAllocator replicaShardAllocator) {
        RoutingNodes.UnassignedShards unassigned = allocation.routingNodes().unassigned();
        unassigned.sort(PriorityComparator.getAllocationComparator(allocation)); // sort for priority ordering

        primaryShardAllocator.allocateUnassigned(allocation);
        replicaShardAllocator.processExistingRecoveries(allocation);
        replicaShardAllocator.allocateUnassigned(allocation);
    }

    /**
     * Computes and returns the design for allocating a single unassigned shard.  If called on an assigned shard,
     * {@link AllocateUnassignedDecision#NOT_TAKEN} is returned.
     */
    public AllocateUnassignedDecision decideUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation) {
        if (unassignedShard.primary()) {
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            return replicaShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        }
    }

    class InternalAsyncFetch<T extends BaseNodeResponse> extends AsyncShardFetch<T> {

        InternalAsyncFetch(Logger logger, String type, ShardId shardId, Lister<? extends BaseNodesResponse<T>, T> action) {
            super(logger, type, shardId, action);
        }

        @Override
        protected void reroute(ShardId shardId, String reason) {
            logger.trace("{} scheduling reroute for {}", shardId, reason);
            routingService.reroute("async_shard_fetch");
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final TransportNodesListGatewayStartedShards startedAction;

        InternalPrimaryShardAllocator(Settings settings, TransportNodesListGatewayStartedShards startedAction) {
            super(settings);
            this.startedAction = startedAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            AsyncShardFetch<TransportNodesListGatewayStartedShards.NodeGatewayStartedShards> fetch =
                asyncFetchStarted.computeIfAbsent(shard.shardId(), shardId -> new InternalAsyncFetch<>(logger, "shard_started", shardId, startedAction));
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

        InternalReplicaShardAllocator(Settings settings, TransportNodesListShardStoreMetaData storeAction) {
            super(settings);
            this.storeAction = storeAction;
        }

        @Override
        protected AsyncShardFetch.FetchResult<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            AsyncShardFetch<TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData> fetch =
                asyncFetchStore.computeIfAbsent(shard.shardId(), shardId -> new InternalAsyncFetch<>(logger, "shard_store", shard.shardId(), storeAction));
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
