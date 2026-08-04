/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
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
            .addAsNew(metadata.getProject().index("idx"))
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
                        IndexMetadata.builder(clusterState.metadata().getProject().index("idx"))
                            .settings(
                                Settings.builder()
                                    .put(clusterState.metadata().getProject().index("idx").getSettings())
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
        assertThat(clusterState.metadata().projects().size(), equalTo(1));
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        // shard could be relocated while retries are not exhausted
        for (int i = 0; i < retries; i++) {
            clusterState = withRoutingAllocation(clusterState, allocation -> {
                var source = allocation.routingTable(projectId).index("idx").shard(0).shard(0);
                var targetNodeId = Objects.equals(source.currentNodeId(), "node1") ? "node2" : "node1";
                assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
                allocation.routingNodes().relocateShard(source, targetNodeId, 0, "test", allocation.changes());
            });
            clusterState = applyShardFailure(
                clusterState,
                clusterState.getRoutingTable().index("idx").shard(0).shard(0).getTargetRelocatingShard(),
                "boom" + i
            );

            var relocationFailureInfo = clusterState.globalRoutingTable()
                .routingTable(projectId)
                .index("idx")
                .shard(0)
                .shard(0)
                .relocationFailureInfo();
            assertThat(relocationFailureInfo.failedRelocations(), equalTo(i + 1));
        }

        // shard could not be relocated when retries are exhausted
        withRoutingAllocation(clusterState, allocation -> {
            allocation.debugDecision(true);
            final var decision = decider.canAllocate(
                allocation.globalRoutingTable().routingTable(projectId).index("idx").shard(0).shard(0),
                allocation
            );
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
            var source = allocation.globalRoutingTable().routingTable(projectId).index("idx").shard(0).shard(0);
            assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
        });
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.AllocationService:DEBUG",
        reason = "verifies recovery cancellation logs at debug, not warn"
    )
    public void testRecoveryCancellation() {
        var clusterState = createInitialClusterState();
        final int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        // Burn through maxRetries - 1 genuine failures, bouncing between node1/node2.
        for (int i = 0; i < maxRetries - 1; i++) {
            clusterState = applyShardFailure(clusterState, clusterState.routingTable().index("idx").shard(0).shard(0), "genuine-" + i);
        }
        final var routingBeforeMove = clusterState.routingTable().index("idx").shard(0).shard(0);
        assertThat(routingBeforeMove.unassignedInfo().failedAllocations(), equalTo(maxRetries - 1));
        final var failedNodeIdsBeforeCancellation = routingBeforeMove.unassignedInfo().failedNodeIds();

        // Move the shard directly onto a fresh node that has never failed.
        final String freshNodeId = randomIdentifier("node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(freshNodeId)))
            .build();
        clusterState = withRoutingAllocation(clusterState, allocation -> {
            final var initializing = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
            allocation.routingNodes().failShard(initializing, initializing.unassignedInfo(), allocation.changes());
            final var unassignedIterator = allocation.routingNodes().unassigned().iterator();
            unassignedIterator.next();
            unassignedIterator.initialize(freshNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE, allocation.changes());
        });

        final var routingBeforeCancellation = clusterState.routingTable().index("idx").shard(0).shard(0);
        final var infoBeforeCancellation = routingBeforeCancellation.unassignedInfo();
        assertThat(routingBeforeCancellation.currentNodeId(), equalTo(freshNodeId));
        assertThat(infoBeforeCancellation.failedAllocations(), equalTo(maxRetries - 1));
        assertThat(infoBeforeCancellation.failedNodeIds(), equalTo(failedNodeIdsBeforeCancellation));
        assertThat("fresh node does not have a failed allocation", failedNodeIdsBeforeCancellation, not(hasItem(freshNodeId)));

        final ClusterState stateBeforeCancellation = clusterState;
        final AtomicReference<ClusterState> stateAfterCancellation = new AtomicReference<>();
        MockLog.assertThatLogger(
            () -> stateAfterCancellation.set(
                applyShardCancellation(stateBeforeCancellation, stateBeforeCancellation.routingTable().index("idx").shard(0).shard(0))
            ),
            AllocationService.class,
            new MockLog.SeenEventExpectation(
                "recovery cancellation logs at debug",
                AllocationService.class.getCanonicalName(),
                Level.DEBUG,
                "recovery cancelled for shard *"
            ),
            new MockLog.UnseenEventExpectation(
                "recovery cancellation must not log at warn",
                AllocationService.class.getCanonicalName(),
                Level.WARN,
                "*"
            )
        );
        clusterState = stateAfterCancellation.get();

        final var routingAfterCancellation = clusterState.routingTable().index("idx").shard(0).shard(0);
        final var updatedInfo = routingAfterCancellation.unassignedInfo();
        assertThat("cancellation should not increment failedAllocations", updatedInfo.failedAllocations(), equalTo(maxRetries - 1));
        assertThat(updatedInfo.reason(), equalTo(UnassignedInfo.Reason.RECOVERY_CANCELLED));
        assertThat(routingAfterCancellation.state(), equalTo(INITIALIZING));
        assertThat("No expected changes to failedNodeIds", updatedInfo.failedNodeIds(), equalTo(failedNodeIdsBeforeCancellation));
    }

    public void testRecoveryCancellationDuringRelocation() {
        ClusterState clusterState = createInitialClusterState();
        final int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        // Burn through maxRetries - 1 genuine relocation failures.
        for (int i = 0; i < maxRetries - 1; i++) {
            clusterState = withRoutingAllocation(clusterState, allocation -> {
                var source = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
                var targetNodeId = "node1".equals(source.currentNodeId()) ? "node2" : "node1";
                assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
                allocation.routingNodes().relocateShard(source, targetNodeId, 0, "test", allocation.changes());
            });
            final var targetShard = clusterState.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0).getTargetRelocatingShard();
            clusterState = applyShardFailure(clusterState, targetShard, "failure-" + i);
        }

        final int failedRelocationsBeforeCancellation = clusterState.globalRoutingTable()
            .routingTable(ProjectId.DEFAULT)
            .index("idx")
            .shard(0)
            .shard(0)
            .relocationFailureInfo()
            .failedRelocations();
        assertThat(failedRelocationsBeforeCancellation, equalTo(maxRetries - 1));

        // Start another relocation onto a fresh node, then cancel it via RecoveryCancelledException.
        final String freshNodeId = randomIdentifier("node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(freshNodeId)))
            .build();
        clusterState = withRoutingAllocation(clusterState, allocation -> {
            var source = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
            assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
            allocation.routingNodes().relocateShard(source, freshNodeId, 0, "test", allocation.changes());
        });

        final var relocatingSource = clusterState.routingTable().index("idx").shard(0).shard(0);
        assertTrue(relocatingSource.relocating());
        final var relocationTarget = relocatingSource.getTargetRelocatingShard();
        assertThat(relocationTarget.currentNodeId(), equalTo(freshNodeId));

        clusterState = applyShardCancellation(clusterState, relocationTarget);

        final var afterCancellation = clusterState.routingTable().index("idx").shard(0).shard(0);
        assertThat(afterCancellation.state(), equalTo(STARTED));
        assertFalse(afterCancellation.relocating());
        assertThat(
            "cancellation should not increment failedRelocations",
            afterCancellation.relocationFailureInfo().failedRelocations(),
            equalTo(failedRelocationsBeforeCancellation)
        );

        // Still one retry remaining, so another relocation must be allowed.
        withRoutingAllocation(clusterState, allocation -> {
            var source = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
            assertThat(decider.canAllocate(source, allocation).type(), equalTo(Decision.Type.YES));
        });

        // Consume the last retry with a genuine failure, relocation must then be blocked.
        clusterState = withRoutingAllocation(clusterState, allocation -> {
            var source = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
            var targetNodeId = "node1".equals(source.currentNodeId()) ? "node2" : "node1";
            allocation.routingNodes().relocateShard(source, targetNodeId, 0, "test", allocation.changes());
        });
        final var lastTarget = clusterState.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0).getTargetRelocatingShard();
        clusterState = applyShardFailure(clusterState, lastTarget, "final-failure");

        final var afterFinalFailure = clusterState.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
        assertThat(afterFinalFailure.relocationFailureInfo().failedRelocations(), equalTo(maxRetries));
        withRoutingAllocation(clusterState, allocation -> {
            allocation.debugDecision(true);
            var source = allocation.routingTable(ProjectId.DEFAULT).index("idx").shard(0).shard(0);
            final var decision = decider.canAllocate(source, allocation);
            assertThat(decision.type(), equalTo(Decision.Type.NO));
            assertThat(decision.getExplanation(), containsString("shard has exceeded the maximum number of retries"));
        });
    }

    public void testPrimaryRelocationDoesNotConsumeReplicaRelocationRetryBudget() {
        final var testStrategy = new AllocationService(
            new AllocationDeciders(List.of(decider, new ReplicaAfterPrimaryActiveAllocationDecider())),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );

        final var metadata = Metadata.builder()
            .put(IndexMetadata.builder("idx").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();
        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(metadata.getProject().index("idx")).build()
            )
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4")))
            .build();

        clusterState = testStrategy.reroute(clusterState, "initial", ActionListener.noop());
        clusterState = startInitializingShardsAndReroute(testStrategy, clusterState); // start primary
        clusterState = startInitializingShardsAndReroute(testStrategy, clusterState); // start replica

        final var initialShard = clusterState.routingTable().index("idx").shard(0);
        final var shardId = initialShard.shardId();
        final var initialPrimary = initialShard.primaryShard();
        final var initialReplicas = initialShard.replicaShards();
        assertThat(initialPrimary.state(), equalTo(STARTED));
        assertThat(initialReplicas.getFirst().state(), equalTo(STARTED));

        final int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        final String primaryNodeId = initialPrimary.currentNodeId();
        final String replicaNodeId = initialReplicas.getFirst().currentNodeId();
        final String replicaAllocationId = initialReplicas.getFirst().allocationId().getId();

        // Pick a target node that is neither the primary's nor the replica's current node.
        final String tempTarget = Stream.of("node1", "node2", "node3", "node4")
            .filter(n -> n.equals(primaryNodeId) == false && n.equals(replicaNodeId) == false)
            .findFirst()
            .get();

        // Burn through maxRetries - 1 genuine relocation failures on the replica.
        for (int i = 0; i < maxRetries - 1; i++) {
            clusterState = withRoutingAllocation(clusterState, alloc -> {
                final var replica = alloc.routingNodes().getByAllocationId(shardId, replicaAllocationId);
                alloc.routingNodes().relocateShard(replica, tempTarget, 0, "test", alloc.changes());
            });
            clusterState = withRoutingAllocation(clusterState, alloc -> {
                final var target = alloc.routingNodes()
                    .assignedShards(shardId)
                    .stream()
                    .filter(ShardRouting::isRelocationTarget)
                    .toList()
                    .getFirst();
                alloc.routingNodes()
                    .failShard(target, new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "failure"), alloc.changes());
            });
        }

        final var shardBeforePrimaryRelocation = clusterState.routingTable().index("idx").shard(0);
        final var replicaBeforePrimaryRelocation = shardBeforePrimaryRelocation.replicaShards().getFirst();
        assertThat(replicaBeforePrimaryRelocation.relocationFailureInfo().failedRelocations(), equalTo(maxRetries - 1));
        assertThat(replicaBeforePrimaryRelocation.allocationId().getId(), equalTo(replicaAllocationId));

        final String freshPrimaryNode = "fresh-primary-target";
        final String freshReplicaNode = "fresh-replica-target";
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode(freshPrimaryNode)).add(newNode(freshReplicaNode)))
            .build();

        // Relocate both the primary and the replica to the fresh nodes.
        final String primaryAllocationId = shardBeforePrimaryRelocation.primaryShard().allocationId().getId();
        clusterState = withRoutingAllocation(clusterState, alloc -> {
            final var primary = alloc.routingNodes().getByAllocationId(shardId, primaryAllocationId);
            final var replica = alloc.routingNodes().getByAllocationId(shardId, replicaAllocationId);
            alloc.routingNodes().relocateShard(primary, freshPrimaryNode, 0, "test", alloc.changes());
            alloc.routingNodes().relocateShard(replica, freshReplicaNode, 0, "test", alloc.changes());
        });

        final String primaryTargetAllocationId = clusterState.routingTable()
            .index("idx")
            .shard(0)
            .primaryShard()
            .getTargetRelocatingShard()
            .allocationId()
            .getId();

        // Start the primary's relocation target. This triggers reinitiation of the replica's relocation.
        clusterState = withRoutingAllocation(clusterState, alloc -> {
            final var primaryTarget = alloc.routingNodes().getByAllocationId(shardId, primaryTargetAllocationId);
            alloc.routingNodes().startShard(primaryTarget, alloc.changes(), primaryTarget.getExpectedShardSize());
        });

        // The replica's relocation should have been restarted without incrementing failedRelocations.
        final var replicaAfterPrimaryStart = clusterState.routingTable().index("idx").shard(0).replicaShards().getFirst();
        assertThat(
            "restarting replica relocation due to primary moving should not increment failedRelocations",
            replicaAfterPrimaryStart.relocationFailureInfo().failedRelocations(),
            equalTo(maxRetries - 1)
        );

        // One retry remains, so the decider must still allow relocation.
        withRoutingAllocation(clusterState, alloc -> {
            final var replicaSource = alloc.routingNodes()
                .assignedShards(shardId)
                .stream()
                .filter(ShardRouting::relocating)
                .findFirst()
                .orElseThrow();
            assertThat(decider.canAllocate(replicaSource, alloc).type(), equalTo(Decision.Type.YES));
        });

        // Consume the last retry with a genuine relocation failure.
        clusterState = withRoutingAllocation(clusterState, alloc -> {
            final var target = alloc.routingNodes()
                .assignedShards(shardId)
                .stream()
                .filter(ShardRouting::isRelocationTarget)
                .toList()
                .getFirst();
            alloc.routingNodes()
                .failShard(target, new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "final-failure"), alloc.changes());
        });

        // All retries exhausted: the decider must block further relocation.
        withRoutingAllocation(clusterState, alloc -> {
            alloc.debugDecision(true);
            final var replicaSource = alloc.routingNodes()
                .assignedShards(shardId)
                .stream()
                .filter(s -> s.primary() == false && s.started())
                .toList()
                .getFirst();
            final var decision = decider.canAllocate(replicaSource, alloc);
            assertThat(decision.type(), equalTo(Decision.Type.NO));
            assertThat(decision.getExplanation(), containsString("shard has exceeded the maximum number of retries"));
        });
    }

    private ClusterState applyShardCancellation(ClusterState clusterState, ShardRouting shardRouting) {
        final var cause = new RecoveryCancelledException(
            shardRouting.shardId(),
            null,
            clusterState.nodes().get(shardRouting.currentNodeId())
        );
        return strategy.applyFailedShards(
            clusterState,
            List.of(new FailedShard(shardRouting, "recovery cancelled", cause, false)),
            List.of()
        );
    }

    private ClusterState applyShardFailure(ClusterState clusterState, ShardRouting shardRouting, String message) {
        return strategy.applyFailedShards(
            clusterState,
            List.of(new FailedShard(shardRouting, message, new RuntimeException("test"), randomBoolean())),
            List.of()
        );
    }

    private static ClusterState withRoutingAllocation(ClusterState clusterState, Consumer<RoutingAllocation> block) {
        RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterState).mutable();
        block.accept(allocation);
        return updateClusterState(clusterState, allocation);
    }

    private static ClusterState updateClusterState(ClusterState state, RoutingAllocation allocation) {
        assert allocation.metadata() == state.metadata();
        if (allocation.routingNodesChanged() == false) {
            return state;
        }

        assertThat(state.metadata().projects(), aMapWithSize(1));

        final GlobalRoutingTable newRoutingTable = state.globalRoutingTable().rebuild(allocation.routingNodes(), allocation.metadata());
        final Metadata newMetadata = allocation.updateMetadataWithRoutingChanges(newRoutingTable);
        assert newRoutingTable.validate(newMetadata);

        return state.copyAndUpdate(builder -> builder.routingTable(newRoutingTable).metadata(newMetadata));
    }

    private RoutingAllocation newRoutingAllocation(ClusterState clusterState) {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(clusterState).build();
        if (randomBoolean()) {
            routingAllocation.setDebugMode(randomFrom(RoutingAllocation.DebugMode.values()));
        }
        return routingAllocation;
    }
}
