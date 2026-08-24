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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryCancelledException;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

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
                allocation.routingNodes()
                    .relocateShard(
                        source,
                        targetNodeId,
                        0,
                        "test",
                        allocation.changes(),
                        ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                    );
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
                allocation.routingNodes()
                    .relocateShard(
                        source,
                        targetNodeId,
                        0,
                        "test",
                        allocation.changes(),
                        ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                    );
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
            allocation.routingNodes()
                .relocateShard(
                    source,
                    freshNodeId,
                    0,
                    "test",
                    allocation.changes(),
                    ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                );
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
            allocation.routingNodes()
                .relocateShard(
                    source,
                    targetNodeId,
                    0,
                    "test",
                    allocation.changes(),
                    ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                );
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

        final var shardId = clusterState.routingTable().index("idx").shard(0).shardId();
        final int maxRetries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);

        // Mix primary moves with genuine replica failures.
        int genuineFailures = 0;
        while (genuineFailures < maxRetries) {
            final var replica = soleReplicaShard(clusterState, 0);
            assertThat(replica.state(), equalTo(STARTED));
            assertThat(replica.relocationFailureInfo().failedRelocations(), equalTo(genuineFailures));
            withRoutingAllocation(
                clusterState,
                alloc -> assertThat(decider.canAllocate(replica, alloc).type(), equalTo(Decision.Type.YES))
            );

            final List<String> freeNodes = unoccupiedNodeIds(clusterState, shardId);
            assertThat(freeNodes.size(), equalTo(2));

            if (randomBoolean()) { // primary move
                final String primaryTargetNode = freeNodes.get(0);
                final String replicaTargetNode = freeNodes.get(1);

                clusterState = withRoutingAllocation(clusterState, alloc -> {
                    final var primary = soleStartedPrimaryShard(alloc, shardId);
                    final var startedReplica = soleStartedReplicaShard(alloc, shardId);
                    alloc.routingNodes()
                        .relocateShard(
                            primary,
                            primaryTargetNode,
                            0,
                            "test",
                            alloc.changes(),
                            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                        );
                    alloc.routingNodes()
                        .relocateShard(
                            startedReplica,
                            replicaTargetNode,
                            0,
                            "test",
                            alloc.changes(),
                            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                        );
                });

                final var primaryTarget = primaryShard(clusterState, 0).getTargetRelocatingShard();
                clusterState = withRoutingAllocation(clusterState, alloc -> {
                    final var target = alloc.routingNodes().getByAllocationId(shardId, primaryTarget.allocationId().getId());
                    alloc.routingNodes().startShard(target, alloc.changes(), target.getExpectedShardSize());
                });

                final var restartedReplica = soleReplicaShard(clusterState, 0);
                assertTrue(restartedReplica.relocating());
                assertThat(
                    "primary move must not consume the replica relocation retry budget",
                    restartedReplica.relocationFailureInfo().failedRelocations(),
                    equalTo(genuineFailures)
                );

                // Cancel the restarted relocation (not a failure) so the next iteration starts from STARTED
                final var replicaTarget = restartedReplica.getTargetRelocatingShard();
                clusterState = withRoutingAllocation(clusterState, alloc -> {
                    final var target = alloc.routingNodes().getByAllocationId(shardId, replicaTarget.allocationId().getId());
                    final var cancelledInfo = new UnassignedInfo(UnassignedInfo.Reason.RECOVERY_CANCELLED, "cleanup");
                    alloc.routingNodes().failShard(target, cancelledInfo, alloc.changes());
                });
            } else { // genuine failure
                final String replicaTargetNode = randomFrom(freeNodes);
                clusterState = withRoutingAllocation(clusterState, alloc -> {
                    final var startedReplica = soleStartedReplicaShard(alloc, shardId);
                    alloc.routingNodes()
                        .relocateShard(
                            startedReplica,
                            replicaTargetNode,
                            0,
                            "test",
                            alloc.changes(),
                            ShardRouting.RecoveryPriority.RELOCATION_CAN_REMAIN_NO
                        );
                });

                final var relocationTarget = soleReplicaShard(clusterState, 0).getTargetRelocatingShard();
                clusterState = withRoutingAllocation(clusterState, alloc -> {
                    final var target = alloc.routingNodes().getByAllocationId(shardId, relocationTarget.allocationId().getId());
                    final var failedInfo = new UnassignedInfo(UnassignedInfo.Reason.ALLOCATION_FAILED, "failure");
                    alloc.routingNodes().failShard(target, failedInfo, alloc.changes());
                });
                genuineFailures++;
            }
        }

        final var exhaustedReplica = soleReplicaShard(clusterState, 0);
        assertThat(exhaustedReplica.state(), equalTo(STARTED));
        assertThat(exhaustedReplica.relocationFailureInfo().failedRelocations(), equalTo(maxRetries));
        withRoutingAllocation(clusterState, alloc -> {
            alloc.debugDecision(true);
            final var decision = decider.canAllocate(exhaustedReplica, alloc);
            assertThat(decision.type(), equalTo(Decision.Type.NO));
            assertThat(decision.getExplanation(), containsString("shard has exceeded the maximum number of retries"));
        });
    }

    private static ShardRouting soleReplicaShard(ClusterState clusterState, int shardId) {
        final var shard = clusterState.routingTable().index("idx").shard(shardId);
        assertThat(shard, notNullValue());
        final var replicas = shard.replicaShards();
        assertThat(replicas, hasSize(1));
        return replicas.getFirst();
    }

    private static ShardRouting primaryShard(ClusterState clusterState, int shardId) {
        final var shard = clusterState.routingTable().index("idx").shard(shardId);
        assertThat(shard, notNullValue());
        return shard.primaryShard();
    }

    private static ShardRouting soleStartedReplicaShard(RoutingAllocation alloc, ShardId shardId) {
        final var filteredShards = alloc.routingNodes()
            .assignedShards(shardId)
            .stream()
            .filter(s -> s.primary() == false && s.started())
            .toList();
        assertThat(filteredShards, hasSize(1));
        return filteredShards.getFirst();
    }

    private static ShardRouting soleStartedPrimaryShard(RoutingAllocation alloc, ShardId shardId) {
        final var filteredShards = alloc.routingNodes().assignedShards(shardId).stream().filter(s -> s.primary() && s.started()).toList();
        assertThat(filteredShards, hasSize(1));
        return filteredShards.getFirst();
    }

    private static List<String> unoccupiedNodeIds(ClusterState clusterState, ShardId shardId) {
        final var occupied = clusterState.getRoutingNodes()
            .assignedShards(shardId)
            .stream()
            .map(ShardRouting::currentNodeId)
            .collect(Collectors.toSet());
        return clusterState.nodes().getNodes().keySet().stream().filter(id -> occupied.contains(id) == false).toList();
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
