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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
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
import org.elasticsearch.gateway.AsyncShardFetch.Lister;
import org.elasticsearch.gateway.TransportNodesListGatewayStartedShards.NodeGatewayStartedShards;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData;
import org.elasticsearch.indices.store.TransportNodesListShardStoreMetaData.NodeStoreFilesMetaData;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

public class GatewayAllocator {

    private static final Logger logger = LogManager.getLogger(GatewayAllocator.class);

    private final RerouteService rerouteService;

    private final PrimaryShardAllocator primaryShardAllocator;
    private final ReplicaShardAllocator replicaShardAllocator;

    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeGatewayStartedShards>>
        asyncFetchStarted = ConcurrentCollections.newConcurrentMap();
    private final ConcurrentMap<ShardId, AsyncShardFetch<NodeStoreFilesMetaData>>
        asyncFetchStore = ConcurrentCollections.newConcurrentMap();

    @Inject
    public GatewayAllocator(RerouteService rerouteService, NodeClient client) {
        this.rerouteService = rerouteService;
        this.primaryShardAllocator = new InternalPrimaryShardAllocator(client);
        this.replicaShardAllocator = new InternalReplicaShardAllocator(client);
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
        for (AsyncShardFetch<NodeGatewayStartedShards> fetch : asyncFetchStarted.values()) {
            count += fetch.getNumberOfInFlightFetches();
        }
        for (AsyncShardFetch<NodeStoreFilesMetaData> fetch : asyncFetchStore.values()) {
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
            assert primaryShardAllocator != null;
            return primaryShardAllocator.makeAllocationDecision(unassignedShard, routingAllocation, logger);
        } else {
            assert replicaShardAllocator != null;
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
            assert rerouteService != null;
            rerouteService.reroute("async_shard_fetch", Priority.HIGH, ActionListener.wrap(
                r -> logger.trace("{} scheduled reroute completed for {}", shardId, reason),
                e -> logger.debug(new ParameterizedMessage("{} scheduled reroute failed for {}", shardId, reason), e)));
        }
    }

    class InternalPrimaryShardAllocator extends PrimaryShardAllocator {

        private final NodeClient client;

        InternalPrimaryShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeGatewayStartedShards> fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            Lister<BaseNodesResponse<NodeGatewayStartedShards>, NodeGatewayStartedShards> lister = this::listStartedShards;
            AsyncShardFetch<NodeGatewayStartedShards> fetch =
                asyncFetchStarted.computeIfAbsent(shard.shardId(),
                            shardId -> new InternalAsyncFetch<>(logger, "shard_started", shardId, lister));
            AsyncShardFetch.FetchResult<NodeGatewayStartedShards> shardState =
                    fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));

            if (shardState.hasData()) {
                shardState.processAllocation(allocation);
            }
            return shardState;
        }

        private void listStartedShards(ShardId shardId, DiscoveryNode[] nodes,
                                       ActionListener<BaseNodesResponse<NodeGatewayStartedShards>> listener) {
            var request = new TransportNodesListGatewayStartedShards.Request(shardId, nodes);
            client.executeLocally(TransportNodesListGatewayStartedShards.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }
    }

    class InternalReplicaShardAllocator extends ReplicaShardAllocator {

        private final NodeClient client;

        InternalReplicaShardAllocator(NodeClient client) {
            this.client = client;
        }

        @Override
        protected AsyncShardFetch.FetchResult<NodeStoreFilesMetaData>
                                                                        fetchData(ShardRouting shard, RoutingAllocation allocation) {
            // explicitely type lister, some IDEs (Eclipse) are not able to correctly infer the function type
            Lister<BaseNodesResponse<NodeStoreFilesMetaData>, NodeStoreFilesMetaData> lister = this::listStoreFilesMetaData;
            AsyncShardFetch<NodeStoreFilesMetaData> fetch = asyncFetchStore.computeIfAbsent(shard.shardId(),
                    shardId -> new InternalAsyncFetch<>(logger, "shard_store", shard.shardId(), lister));
            AsyncShardFetch.FetchResult<NodeStoreFilesMetaData> shardStores =
                    fetch.fetchData(allocation.nodes(), allocation.getIgnoreNodes(shard.shardId()));
            if (shardStores.hasData()) {
                shardStores.processAllocation(allocation);
            }
            return shardStores;
        }

        private void listStoreFilesMetaData(ShardId shardId, DiscoveryNode[] nodes,
                                            ActionListener<BaseNodesResponse<NodeStoreFilesMetaData>> listener) {
            var request = new TransportNodesListShardStoreMetaData.Request(shardId, nodes);
            client.executeLocally(TransportNodesListShardStoreMetaData.TYPE, request,
                ActionListener.wrap(listener::onResponse, listener::onFailure));
        }

        @Override
        protected boolean hasInitiatedFetching(ShardRouting shard) {
            return asyncFetchStore.get(shard.shardId()) != null;
        }
    }
}
