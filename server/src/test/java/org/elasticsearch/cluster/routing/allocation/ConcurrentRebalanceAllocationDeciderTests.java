/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * A series of tests that set up cluster state with a specified number of relocations in progress.
 *
 * Then sets and checks the ConcurrentRebalanceAllocationDecider canRebalance logic, at both the cluster-
 * and shard-level.
 */
public class ConcurrentRebalanceAllocationDeciderTests extends ESAllocationTestCase {

    public void testConcurrentRelocationsAllowed() {
        final int relocations = randomIntBetween(2, 20);
        final int relocationLimit = relocations + randomIntBetween(1, 10);
        ClusterState clusterState = setupConcurrentRelocations(relocations);

        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 0)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [cluster.routing.allocation.cluster_concurrent_rebalance=-1] for concurrent rebalances, "
                    + "current rebalance shard count [%d]",
                relocations
            )
        );
        assertShardAllocationDecision(clusterState, settings, Decision.Type.YES, "unlimited concurrent rebalances are allowed");

        settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", relocationLimit)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 0)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [cluster.routing.allocation.cluster_concurrent_rebalance=%d] for concurrent rebalances, "
                    + "current rebalance shard count [%d]",
                relocationLimit,
                relocations
            )
        );
        assertShardAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [%d] for concurrent rebalances, current rebalance shard count [%d]",
                relocationLimit,
                relocations
            )
        );
    }

    public void testFrozenConcurrentRelocationsAllowed() {
        final int relocations = randomIntBetween(2, 20);
        final int relocationLimit = relocations + randomIntBetween(1, 10);
        ClusterState clusterState = setupConcurrentFrozenRelocations(relocations);

        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 0)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", -1)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [cluster.routing.allocation.cluster_concurrent_frozen_rebalance=-1] for concurrent frozen "
                    + "rebalances, current frozen rebalance shard count [%d]",
                relocations
            )
        );
        assertShardAllocationDecision(clusterState, settings, Decision.Type.YES, "unlimited concurrent frozen rebalances are allowed");

        settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 0)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", relocationLimit)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [cluster.routing.allocation.cluster_concurrent_frozen_rebalance=%d] for concurrent frozen "
                    + "rebalances, current frozen rebalance shard count [%d]",
                relocationLimit,
                relocations
            )
        );
        assertShardAllocationDecision(
            clusterState,
            settings,
            Decision.Type.YES,
            Strings.format(
                "below threshold [%d] for concurrent frozen rebalances, current frozen rebalance shard count [%d]",
                relocationLimit,
                relocations
            )
        );
    }

    public void testThrottleDecision() {
        final int relocations = randomIntBetween(2, 20);
        final int relocationThrottleLimit = relocations - randomIntBetween(0, relocations - 1);
        ClusterState clusterState = setupConcurrentRelocations(relocations);

        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", relocationThrottleLimit)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 0)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.THROTTLE,
            Strings.format(
                "reached the limit of concurrently rebalancing shards [%d] for concurrent rebalances, "
                    + "cluster setting [cluster.routing.allocation.cluster_concurrent_rebalance=%d], "
                    + "and [0] for concurrent frozen rebalances, frozen cluster setting "
                    + "[cluster.routing.allocation.cluster_concurrent_frozen_rebalance=0]",
                relocations,
                relocationThrottleLimit
            )
        );
        assertShardAllocationDecision(
            clusterState,
            settings,
            Decision.Type.THROTTLE,
            Strings.format(
                "reached the limit of concurrently rebalancing shards [%d], "
                    + "cluster setting [cluster.routing.allocation.cluster_concurrent_rebalance=%d]",
                relocations,
                relocationThrottleLimit
            )
        );
    }

    public void testFrozenThrottleDecision() {
        final int relocations = randomIntBetween(2, 20);
        final int relocationThrottleLimit = relocations - randomIntBetween(0, relocations - 1);
        ClusterState clusterState = setupConcurrentFrozenRelocations(relocations);

        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 0)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", relocationThrottleLimit)
            .build();
        assertClusterAllocationDecision(
            clusterState,
            settings,
            Decision.Type.THROTTLE,
            Strings.format(
                "reached the limit of concurrently rebalancing shards [0] for concurrent rebalances, "
                    + "cluster setting [cluster.routing.allocation.cluster_concurrent_rebalance=0], "
                    + "and [%d] for concurrent frozen rebalances, "
                    + "frozen cluster setting [cluster.routing.allocation.cluster_concurrent_frozen_rebalance=%d]",
                relocations,
                relocationThrottleLimit
            )
        );
        assertShardAllocationDecision(
            clusterState,
            settings,
            Decision.Type.THROTTLE,
            Strings.format(
                "reached the limit of concurrently rebalancing frozen shards [%d], "
                    + "cluster setting [cluster.routing.allocation.cluster_concurrent_frozen_rebalance=%d]",
                relocations,
                relocationThrottleLimit
            )
        );
    }

    private void assertShardAllocationDecision(
        ClusterState clusterState,
        Settings settings,
        Decision.Type decisionType,
        String explanation
    ) {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        ConcurrentRebalanceAllocationDecider decider = new ConcurrentRebalanceAllocationDecider(clusterSettings);

        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Arrays.asList(decider)),
            null,
            clusterState,
            null,
            null,
            0
        );
        allocation.debugDecision(true);

        ShardRouting shardRouting = findStartedShard(clusterState);
        Decision decision = decider.canRebalance(shardRouting, allocation);

        assertThat(decision.type(), equalTo(decisionType));
        assertThat(decision.getExplanation(), containsString(explanation));
    }

    private ShardRouting findStartedShard(ClusterState clusterState) {
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        Map<ShardId, List<ShardRouting>> assignedShards = routingNodes.getAssignedShards();
        for (Map.Entry<ShardId, List<ShardRouting>> entry : assignedShards.entrySet()) {
            for (ShardRouting shardRouting : entry.getValue()) {
                if (shardRouting.state() == ShardRoutingState.STARTED) {
                    return shardRouting;
                }
            }
        }
        assert false : "need at least one started shard";
        return null;
    }

    private void assertClusterAllocationDecision(
        ClusterState clusterState,
        Settings settings,
        Decision.Type decisionType,
        String explanation
    ) {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings(settings);

        ConcurrentRebalanceAllocationDecider decider = new ConcurrentRebalanceAllocationDecider(clusterSettings);

        RoutingAllocation allocation = new RoutingAllocation(
            new AllocationDeciders(Arrays.asList(decider)),
            null,
            clusterState,
            null,
            null,
            0
        );
        allocation.debugDecision(true);

        Decision decision = decider.canRebalance(allocation);

        assertThat(decision.type(), equalTo(decisionType));
        assertThat(decision.getExplanation(), containsString(explanation));
    }

    private Metadata initializeMetadata(int numberOfRelocations) {
        return Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(numberOfRelocations)
                    .numberOfReplicas(1)
            )
            .build();
    }

    private Supplier<DiscoveryNode> nodeFactory() {
        return new Supplier<DiscoveryNode>() {
            int count = 1;

            @Override
            public DiscoveryNode get() {
                return ESAllocationTestCase.newNode("node" + count++);
            }
        };
    }

    private Metadata initializeFrozenMetadata(int numberOfRelocations) {
        return Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN))
                    .numberOfShards(numberOfRelocations)
                    .numberOfReplicas(1)
            )
            .build();
    }

    private Supplier<DiscoveryNode> frozenNodeFactory() {
        return new Supplier<DiscoveryNode>() {
            int count = 1;
            Set<DiscoveryNodeRole> frozenRole = Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);

            @Override
            public DiscoveryNode get() {
                return ESAllocationTestCase.newNode("node" + count++, frozenRole);
            }
        };
    }

    private ClusterState setupConcurrentRelocations(int relocations) {
        return setupConcurrentRelocationsInternal(initializeMetadata(relocations), nodeFactory(), relocations);
    }

    private ClusterState setupConcurrentFrozenRelocations(int relocations) {
        return setupConcurrentRelocationsInternal(initializeFrozenMetadata(relocations), frozenNodeFactory(), relocations);
    }

    /**
     * Set up a cluster state so that a specified number of concurrent relocations are in progress
     *
     * Internally, this creates a bunch of shards (the number of relocations) and one replica,
     * each on their own node. The primaries are all started, then the replicas are set up to all
     * relocate to new nodes.
     */
    private ClusterState setupConcurrentRelocationsInternal(Metadata metadata, Supplier<DiscoveryNode> nodeFactory, int relocations) {
        assert relocations > 1 : "logic only works for 2 or more relocations as the replica needs to initialize elsewhere then relocate";

        AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", -1)
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        var nodeBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < relocations; i++) {
            nodeBuilder = nodeBuilder.add(nodeFactory.get());
        }

        clusterState = ClusterState.builder(clusterState).nodes(nodeBuilder).build();

        // set up primaries, and have replicas initializing
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        // add a bunch of nodes to create relocation chaos
        var clusterStateBuilder = ClusterState.builder(clusterState);
        nodeBuilder = DiscoveryNodes.builder(clusterStateBuilder.nodes());
        for (int i = 0; i < relocations; i++) {
            nodeBuilder = nodeBuilder.add(nodeFactory.get());
        }
        clusterState = clusterStateBuilder.nodes(nodeBuilder).build();

        // start relocations
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        return clusterState;
    }
}
