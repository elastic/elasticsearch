/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class DesiredBalanceShardsAllocator implements ShardsAllocator {

    private final BalancedShardsAllocator balancedShardsAllocator;
    private final Supplier<RerouteService> rerouteServiceSupplier;

    private final ContinuousComputation<RerouteInput> desiredBalanceComputation;

    private volatile DesiredBalance currentDesiredBalance;

    public DesiredBalanceShardsAllocator(
        Settings settings,
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        Supplier<RerouteService> rerouteServiceSupplier
    ) {
        this.rerouteServiceSupplier = rerouteServiceSupplier;
        this.balancedShardsAllocator = new BalancedShardsAllocator(settings, clusterSettings);

        this.desiredBalanceComputation = new ContinuousComputation<>(threadPool.generic()) {
            @Override
            protected void processInput(RerouteInput input) {
                computeDesiredBalance(input);
            }
        };
    }

    public static boolean needsRefresh(ClusterState currentState, ClusterState newState) {

        final Set<String> currentDataNodeEphemeralIds = currentState.nodes()
            .stream()
            .filter(DiscoveryNode::canContainData)
            .map(DiscoveryNode::getEphemeralId)
            .collect(Collectors.toSet());
        final Set<String> newDataNodeEphemeralIds = newState.nodes()
            .stream()
            .filter(DiscoveryNode::canContainData)
            .map(DiscoveryNode::getEphemeralId)
            .collect(Collectors.toSet());

        if (currentDataNodeEphemeralIds.equals(newDataNodeEphemeralIds) == false) {
            return true;
        }

        return false;
    }

    @Override
    public void allocate(RoutingAllocation allocation) {
        assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
            : Thread.currentThread().getName();
        // assert allocation.debugDecision() == false; set to true when called via the reroute API
        assert allocation.ignoreDisable() == false;

        desiredBalanceComputation.onNewInput(new RerouteInput(allocation.nodes(), allocation.metadata(), allocation.routingTable()));

        // 2. compute next moves towards current desired balance

    }

    @Override
    public ShardAllocationDecision decideShardAllocation(ShardRouting shard, RoutingAllocation allocation) {
        return balancedShardsAllocator.decideShardAllocation(shard, allocation);
    }

    record RerouteInput(DiscoveryNodes discoveryNodes, Metadata metadata, RoutingTable routingTable) {}

    private void computeDesiredBalance(RerouteInput input) {
        currentDesiredBalance = new DesiredBalance();
    }

    static class DesiredBalance {

    }
}
