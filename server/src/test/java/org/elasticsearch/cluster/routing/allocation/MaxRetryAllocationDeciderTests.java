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
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class MaxRetryAllocationDeciderTests extends ESAllocationTestCase {

    private final MaxRetryAllocationDecider decider = new MaxRetryAllocationDecider();
    private final AllocationService strategy = new AllocationService(
        new AllocationDeciders(List.of(decider)),
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE,
        TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
    );

    private ClusterState createInitialClusterState() {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("idx").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .build();
        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("idx"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        assertEquals(clusterState.routingTable().index("idx").size(), 1);
        assertEquals(clusterState.routingTable().index("idx").shard(0).shard(0).state(), UNASSIGNED);

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        assertEquals(clusterState.routingTable().index("idx").size(), 1);
        assertEquals(clusterState.routingTable().index("idx").shard(0).shard(0).state(), INITIALIZING);
        return clusterState;
    }

    public void testSingleRetryOnIgnore() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = clusterState.routingTable();
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries - 1; i++) {
            ClusterState newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom" + i);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").size(), 1);
            assertEquals(routingTable.index("idx").shard(0).shard(0).state(), INITIALIZING);
            assertEquals(routingTable.index("idx").shard(0).shard(0).unassignedInfo().failedAllocations(), i + 1);
            assertThat(routingTable.index("idx").shard(0).shard(0).unassignedInfo().message(), containsString("boom" + i));
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        ClusterState newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").size(), 1);
        assertEquals(routingTable.index("idx").shard(0).shard(0).unassignedInfo().failedAllocations(), retries);
        assertEquals(routingTable.index("idx").shard(0).shard(0).state(), UNASSIGNED);
        assertThat(routingTable.index("idx").shard(0).shard(0).unassignedInfo().message(), containsString("boom"));

        // manual resetting of retry count
        newState = strategy.reroute(clusterState, new AllocationCommands(), false, true, false, ActionListener.noop()).clusterState();
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();

        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertEquals(routingTable.index("idx").size(), 1);
        assertEquals(0, routingTable.index("idx").shard(0).shard(0).unassignedInfo().failedAllocations());
        assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shard(0).state());
        assertThat(routingTable.index("idx").shard(0).shard(0).unassignedInfo().message(), containsString("boom"));

        // again fail it N-1 times
        for (int i = 0; i < retries - 1; i++) {
            newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom");
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").size(), 1);
            assertEquals(i + 1, routingTable.index("idx").shard(0).shard(0).unassignedInfo().failedAllocations());
            assertEquals(INITIALIZING, routingTable.index("idx").shard(0).shard(0).state());
            assertThat(routingTable.index("idx").shard(0).shard(0).unassignedInfo().message(), containsString("boom"));
        }

        // now we go and check that we are actually stick to unassigned on the next failure
        newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").size(), 1);
        assertEquals(retries, routingTable.index("idx").shard(0).shard(0).unassignedInfo().failedAllocations());
        assertEquals(UNASSIGNED, routingTable.index("idx").shard(0).shard(0).state());
        assertThat(routingTable.index("idx").shard(0).shard(0).unassignedInfo().message(), containsString("boom"));
    }

    public void testFailedAllocation() {
        ClusterState clusterState = createInitialClusterState();
        RoutingTable routingTable = clusterState.routingTable();
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        // now fail it N-1 times
        for (int i = 0; i < retries - 1; i++) {
            ClusterState newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom" + i);
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").size(), 1);
            ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shard(0);
            assertEquals(unassignedPrimary.state(), INITIALIZING);
            assertEquals(unassignedPrimary.unassignedInfo().failedAllocations(), i + 1);
            assertThat(unassignedPrimary.unassignedInfo().message(), containsString("boom" + i));
            // MaxRetryAllocationDecider#canForceAllocatePrimary should return YES decisions because canAllocate returns YES here
            assertEquals(
                Decision.Type.YES,
                decider.canForceAllocatePrimary(unassignedPrimary, null, newRoutingAllocation(clusterState)).type()
            );
        }
        // now we go and check that we are actually stick to unassigned on the next failure
        {
            ClusterState newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "boom");
            assertThat(newState, not(equalTo(clusterState)));
            clusterState = newState;
            routingTable = newState.routingTable();
            assertEquals(routingTable.index("idx").size(), 1);
            ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shard(0);
            assertEquals(unassignedPrimary.unassignedInfo().failedAllocations(), retries);
            assertEquals(unassignedPrimary.state(), UNASSIGNED);
            assertThat(unassignedPrimary.unassignedInfo().message(), containsString("boom"));
            // MaxRetryAllocationDecider#canForceAllocatePrimary should return a NO decision because canAllocate returns NO here
            final var allocation = newRoutingAllocation(clusterState);
            allocation.debugDecision(true);
            final var decision = decider.canForceAllocatePrimary(unassignedPrimary, null, allocation);
            assertEquals(Decision.Type.NO, decision.type());
            assertThat(
                decision.getExplanation(),
                allOf(
                    containsString("shard has exceeded the maximum number of retries"),
                    containsString("POST /_cluster/reroute?retry_failed")
                )
            );
        }

        // change the settings and ensure we can do another round of allocation for that index.
        clusterState = ClusterState.builder(clusterState)
            .routingTable(routingTable)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(
                        IndexMetadata.builder(clusterState.metadata().index("idx"))
                            .settings(
                                Settings.builder()
                                    .put(clusterState.metadata().index("idx").getSettings())
                                    .put("index.allocation.max_retries", retries + 1)
                                    .build()
                            )
                            .build(),
                        true
                    )
                    .build()
            )
            .build();
        ClusterState newState = strategy.reroute(clusterState, "settings changed", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        // good we are initializing and we are maintaining failure information
        assertEquals(routingTable.index("idx").size(), 1);
        ShardRouting unassignedPrimary = routingTable.index("idx").shard(0).shard(0);
        assertEquals(unassignedPrimary.unassignedInfo().failedAllocations(), retries);
        assertEquals(unassignedPrimary.state(), INITIALIZING);
        assertThat(unassignedPrimary.unassignedInfo().message(), containsString("boom"));
        // bumped up the max retry count, so canForceAllocatePrimary should return a YES decision
        assertEquals(
            Decision.Type.YES,
            decider.canForceAllocatePrimary(routingTable.index("idx").shard(0).shard(0), null, newRoutingAllocation(clusterState)).type()
        );

        // now we start the shard
        clusterState = startShardsAndReroute(strategy, clusterState, routingTable.index("idx").shard(0).shard(0));
        routingTable = clusterState.routingTable();

        // all counters have been reset to 0 ie. no unassigned info
        assertEquals(routingTable.index("idx").size(), 1);
        assertNull(routingTable.index("idx").shard(0).shard(0).unassignedInfo());
        assertEquals(routingTable.index("idx").shard(0).shard(0).state(), STARTED);

        // now fail again and see if it has a new counter
        newState = applyShardFailure(clusterState, routingTable.index("idx").shard(0).shard(0), "ZOOOMG");
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;
        routingTable = newState.routingTable();
        assertEquals(routingTable.index("idx").size(), 1);
        unassignedPrimary = routingTable.index("idx").shard(0).shard(0);
        assertEquals(unassignedPrimary.unassignedInfo().failedAllocations(), 1);
        assertEquals(unassignedPrimary.state(), UNASSIGNED);
        assertThat(unassignedPrimary.unassignedInfo().message(), containsString("ZOOOMG"));
        // Counter reset, so MaxRetryAllocationDecider#canForceAllocatePrimary should return a YES decision
        assertEquals(
            Decision.Type.YES,
            decider.canForceAllocatePrimary(unassignedPrimary, null, newRoutingAllocation(clusterState)).type()
        );
    }

    public void testFailedRelocation() {
        ClusterState clusterState = createInitialClusterState();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        // shard could be relocated while retries are not exhausted
        for (int i = 0; i < retries; i++) {
            clusterState = withRoutingAllocation(clusterState, allocation -> {
                var source = allocation.routingTable().index("idx").shard(0).shard(0);
                var targetNodeId = Objects.equals(source.currentNodeId(), "node1") ? "node2" : "node1";
                assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
                allocation.routingNodes().relocateShard(source, targetNodeId, 0, "test", allocation.changes());
            });
            clusterState = applyShardFailure(
                clusterState,
                clusterState.getRoutingTable().index("idx").shard(0).shard(0).getTargetRelocatingShard(),
                "boom" + i
            );

            var relocationFailureInfo = clusterState.getRoutingTable().index("idx").shard(0).shard(0).relocationFailureInfo();
            assertThat(relocationFailureInfo.failedRelocations(), equalTo(i + 1));
        }

        // shard could not be relocated when retries are exhausted
        withRoutingAllocation(clusterState, allocation -> {
            allocation.debugDecision(true);
            final var decision = decider.canAllocate(allocation.routingTable().index("idx").shard(0).shard(0), allocation);
            assertThat(decision.type(), equalTo(Decision.Type.NO));
            assertThat(
                decision.getExplanation(),
                allOf(
                    containsString("shard has exceeded the maximum number of retries"),
                    containsString("POST /_cluster/reroute?retry_failed")
                )
            );
        });

        // manually reset retry count
        clusterState = strategy.reroute(clusterState, new AllocationCommands(), false, true, false, ActionListener.noop()).clusterState();

        // shard could be relocated again
        withRoutingAllocation(clusterState, allocation -> {
            var source = allocation.routingTable().index("idx").shard(0).shard(0);
            assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
        });
    }

    private ClusterState applyShardFailure(ClusterState clusterState, ShardRouting shardRouting, String message) {
        return strategy.applyFailedShards(
            clusterState,
            List.of(new FailedShard(shardRouting, message, new RuntimeException("test"), randomBoolean())),
            List.of()
        );
    }

    private static ClusterState withRoutingAllocation(ClusterState clusterState, Consumer<RoutingAllocation> block) {
        RoutingAllocation allocation = new RoutingAllocation(
            null,
            clusterState.mutableRoutingNodes(),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            0L
        );
        block.accept(allocation);
        return updateClusterState(clusterState, allocation);
    }

    private static ClusterState updateClusterState(ClusterState state, RoutingAllocation allocation) {
        assert allocation.metadata() == state.metadata();
        if (allocation.routingNodesChanged() == false) {
            return state;
        }
        final RoutingTable newRoutingTable = RoutingTable.of(allocation.routingNodes());
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata);

        return state.copyAndUpdate(builder -> builder.routingTable(newRoutingTable).metadata(newMetadata));
    }

    private RoutingAllocation newRoutingAllocation(ClusterState clusterState) {
        final var routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        if (randomBoolean()) {
            routingAllocation.setDebugMode(randomFrom(RoutingAllocation.DebugMode.values()));
        }
        return routingAllocation;
    }
}
