/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.benchmark.routing.allocation;

import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.FailedShard;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision.NOT_TAKEN;

public final class Allocators {
    private static class NoopGatewayAllocator extends GatewayAllocator {
        public static final NoopGatewayAllocator INSTANCE = new NoopGatewayAllocator();

        @Override
        public void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation) {
            // noop
        }

        @Override
        public void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation) {
            // noop
        }

        @Override
        public void allocateUnassigned(
            ShardRouting shardRouting,
            RoutingAllocation allocation,
            UnassignedAllocationHandler unassignedAllocationHandler
        ) {
            // noop
        }

        @Override
        public AllocateUnassignedDecision explainUnassignedShardAllocation(
            ShardRouting unassignedShard,
            RoutingAllocation routingAllocation
        ) {
            return NOT_TAKEN;
        }
    }

    private Allocators() {
        throw new AssertionError("Do not instantiate");
    }

    public static AllocationService createAllocationService(Settings settings) {
        return createAllocationService(settings, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
    }

    public static AllocationService createAllocationService(Settings settings, ClusterSettings clusterSettings) {
        return new AllocationService(
            defaultAllocationDeciders(settings, clusterSettings),
            NoopGatewayAllocator.INSTANCE,
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
    }

    public static AllocationDeciders defaultAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        Collection<AllocationDecider> deciders = ClusterModule.createAllocationDeciders(settings, clusterSettings, Collections.emptyList());
        return new AllocationDeciders(deciders);
    }

    private static final AtomicInteger portGenerator = new AtomicInteger();

    public static DiscoveryNode newNode(String nodeId, Map<String, String> attributes) {
        return new DiscoveryNode(
            null,
            nodeId,
            new TransportAddress(TransportAddress.META_ADDRESS, portGenerator.incrementAndGet()),
            attributes,
            Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.DATA_ROLE),
            (VersionInformation) null
        );
    }
}
