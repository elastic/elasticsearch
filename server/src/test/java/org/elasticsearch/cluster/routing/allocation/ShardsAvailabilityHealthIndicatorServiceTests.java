/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorImpact;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.RESTART;
import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.AVAILABLE;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.INITIALIZING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.RESTARTING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.UNAVAILABLE;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    public void testShouldBeGreenWhenAllPrimariesAndReplicasAreStarted() {
        var clusterState = createClusterStateWith(
            List.of(
                index("replicated-index", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), AVAILABLE)),
                index("unreplicated-index", new ShardAllocation(randomNodeId(), AVAILABLE))
            ),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has all shards available.",
                    Map.of("started_primaries", 2, "started_replicas", 1),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testShouldBeYellowWhenThereAreUnassignedReplicas() {
        var availableReplicas = randomList(0, 5, () -> new ShardAllocation(randomNodeId(), AVAILABLE));
        var unavailableReplicas = randomList(1, 5, () -> new ShardAllocation(randomNodeId(), UNAVAILABLE));

        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "yellow-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    concatLists(availableReplicas, unavailableReplicas).toArray(ShardAllocation[]::new)
                )
            ),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    unavailableReplicas.size() > 1
                        ? "This cluster has " + unavailableReplicas.size() + " unavailable replicas."
                        : "This cluster has 1 unavailable replica.",
                    Map.of(
                        "started_primaries",
                        1,
                        "unassigned_replicas",
                        unavailableReplicas.size(),
                        "started_replicas",
                        availableReplicas.size()
                    ),
                    List.of(
                        new HealthIndicatorImpact(
                            3,
                            "Searches might return slower than usual. Fewer redundant copies of the data exist on 1 index [yellow-index]."
                        )
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndAssignedReplicas() {
        var clusterState = createClusterStateWith(
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE), new ShardAllocation(randomNodeId(), AVAILABLE))),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary.",
                    Map.of("unassigned_primaries", 1, "started_replicas", 1),
                    List.of(
                        new HealthIndicatorImpact(1, "Cannot add data to 1 index [red-index]. Searches might return incomplete results.")
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndNoReplicas() {
        var clusterState = createClusterStateWith(List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE))), List.of());
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary.",
                    Map.of("unassigned_primaries", 1),
                    List.of(
                        new HealthIndicatorImpact(1, "Cannot add data to 1 index [red-index]. Searches might return incomplete results.")
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicasOnSameIndex() {
        var clusterState = createClusterStateWith(
            List.of(index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        HealthIndicatorResult result = service.calculate();
        assertEquals(RED, result.status());
        assertEquals("This cluster has 1 unavailable primary, 1 unavailable replica.", result.summary());
        assertEquals(1, result.impacts().size());
        assertEquals(
            result.impacts().get(0),
            new HealthIndicatorImpact(1, "Cannot add data to 1 index [red-index]. Searches might return incomplete results.")
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimariesAndUnassignedReplicasOnDifferentIndices() {
        var clusterState = createClusterStateWith(
            List.of(
                index("red-index", new ShardAllocation(randomNodeId(), UNAVAILABLE), new ShardAllocation(randomNodeId(), AVAILABLE)),
                index("yellow-index-1", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE)),
                index("yellow-index-2", new ShardAllocation(randomNodeId(), AVAILABLE), new ShardAllocation(randomNodeId(), UNAVAILABLE))
            ),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        HealthIndicatorResult result = service.calculate();
        assertEquals(RED, result.status());
        assertEquals("This cluster has 1 unavailable primary, 2 unavailable replicas.", result.summary());
        assertEquals(2, result.impacts().size());
        assertEquals(
            result.impacts().get(0),
            new HealthIndicatorImpact(1, "Cannot add data to 1 index [red-index]. Searches might return incomplete results.")
        );
        assertThat(
            result.impacts().get(1),
            oneOf(
                new HealthIndicatorImpact(
                    3,
                    "Searches might return slower than usual. Fewer redundant copies of the data exist on 2 indices [yellow-index-1, "
                        + "yellow-index-2]."
                ),
                new HealthIndicatorImpact(
                    3,
                    "Searches might return slower than usual. Fewer redundant copies of the data exist on 2 indices [yellow-index-2, "
                        + "yellow-index-1]."
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreRestartingReplicas() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 restarting replica.",
                    Map.of("started_primaries", 1, "restarting_replicas", 1),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreNoReplicasExpected() {
        var clusterState = createClusterStateWith(
            List.of(index("primaries-only-index", new ShardAllocation(randomNodeId(), AVAILABLE))),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has all shards available.",
                    Map.of("started_primaries", 1),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testShouldBeYellowWhenRestartingReplicasReachedAllocationDelay() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation(randomNodeId(), AVAILABLE),
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime() - timeValueSeconds(between(60, 180)).nanos())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    "This cluster has 1 unavailable replica.",
                    Map.of("started_primaries", 1, "unassigned_replicas", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            3,
                            "Searches might return slower than usual. Fewer redundant copies of the data exist on 1 index "
                                + "[restarting-index]."
                        )
                    )
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreInitializingPrimaries() {
        var clusterState = createClusterStateWith(
            List.of(index("restarting-index", new ShardAllocation("node-0", INITIALIZING))),
            List.of()
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 creating primary.",
                    Map.of("creating_primaries", 1),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreRestartingPrimaries() {
        var clusterState = createClusterStateWith(
            List.of(index("restarting-index", new ShardAllocation("node-0", RESTARTING, System.nanoTime()))),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 restarting primary.",
                    Map.of("restarting_primaries", 1),
                    Collections.emptyList()
                )
            )
        );
    }

    public void testShouldBeRedWhenRestartingPrimariesReachedAllocationDelayAndNoReplicas() {
        var clusterState = createClusterStateWith(
            List.of(
                index(
                    "restarting-index",
                    new ShardAllocation("node-0", RESTARTING, System.nanoTime() - timeValueSeconds(between(60, 120)).nanos())
                )
            ),
            List.of(new NodeShutdown("node-0", RESTART, 60))
        );
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary.",
                    Map.of("unassigned_primaries", 1),
                    List.of(
                        new HealthIndicatorImpact(
                            1,
                            "Cannot add data to 1 index [restarting-index]. Searches might return incomplete results."
                        )
                    )
                )
            )
        );
    }

    private HealthIndicatorResult createExpectedResult(
        HealthStatus status,
        String summary,
        Map<String, Object> details,
        List<HealthIndicatorImpact> impacts
    ) {
        return new HealthIndicatorResult(NAME, DATA, status, summary, new SimpleHealthIndicatorDetails(addDefaults(details)), impacts);
    }

    private static ClusterState createClusterStateWith(List<IndexRoutingTable> indexes, List<NodeShutdown> nodeShutdowns) {
        var builder = RoutingTable.builder();
        for (IndexRoutingTable index : indexes) {
            builder.add(index);
        }

        var nodesShutdownMetadata = new NodesShutdownMetadata(
            nodeShutdowns.stream()
                .collect(
                    toMap(
                        it -> it.nodeId,
                        it -> SingleNodeShutdownMetadata.builder()
                            .setNodeId(it.nodeId)
                            .setType(it.type)
                            .setReason("test")
                            .setNodeSeen(true)
                            .setStartedAtMillis(System.currentTimeMillis())
                            .setAllocationDelay(it.allocationDelaySeconds != null ? timeValueSeconds(it.allocationDelaySeconds) : null)
                            .build()
                    )
                )
        );

        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(builder.build())
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    private static Map<String, Object> addDefaults(Map<String, Object> override) {
        return Map.of(
            "unassigned_primaries",
            override.getOrDefault("unassigned_primaries", 0),
            "initializing_primaries",
            override.getOrDefault("initializing_primaries", 0),
            "creating_primaries",
            override.getOrDefault("creating_primaries", 0),
            "restarting_primaries",
            override.getOrDefault("restarting_primaries", 0),
            "started_primaries",
            override.getOrDefault("started_primaries", 0),
            "unassigned_replicas",
            override.getOrDefault("unassigned_replicas", 0),
            "initializing_replicas",
            override.getOrDefault("initializing_replicas", 0),
            "restarting_replicas",
            override.getOrDefault("restarting_replicas", 0),
            "started_replicas",
            override.getOrDefault("started_replicas", 0)
        );
    }

    private static IndexRoutingTable index(String name, ShardAllocation primaryState, ShardAllocation... replicaStates) {
        var index = new Index(name, UUID.randomUUID().toString());
        var shardId = new ShardId(index, 0);

        var builder = IndexRoutingTable.builder(index);
        builder.addShard(createShardRouting(shardId, true, primaryState));
        for (var replicaState : replicaStates) {
            builder.addShard(createShardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static ShardRouting createShardRouting(ShardId shardId, boolean primary, ShardAllocation allocation) {
        var routing = newUnassigned(
            shardId,
            primary,
            getSource(primary, allocation.state),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        if (allocation.state == UNAVAILABLE || allocation.state == INITIALIZING) {
            return routing;
        }
        routing = routing.initialize(allocation.nodeId, null, 0);
        routing = routing.moveToStarted();
        if (allocation.state == AVAILABLE) {
            return routing;
        }
        routing = routing.moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_RESTARTING,
                null,
                null,
                -1,
                allocation.unassignedTimeNanos != null ? allocation.unassignedTimeNanos : 0,
                0,
                false,
                UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                Set.of(),
                allocation.nodeId
            )
        );
        if (allocation.state == RESTARTING) {
            return routing;
        }

        throw new AssertionError("Unexpected state [" + allocation.state + "]");
    }

    private static RecoverySource getSource(boolean primary, ShardState state) {
        if (primary) {
            return state == INITIALIZING
                ? RecoverySource.EmptyStoreRecoverySource.INSTANCE
                : RecoverySource.ExistingStoreRecoverySource.INSTANCE;
        } else {
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }
    }

    public enum ShardState {
        UNAVAILABLE,
        INITIALIZING,
        AVAILABLE,
        RESTARTING
    }

    private record ShardAllocation(String nodeId, ShardState state, Long unassignedTimeNanos) {

        ShardAllocation(String nodeId, ShardState state) {
            this(nodeId, state, null);
        }
    }

    private record NodeShutdown(String nodeId, SingleNodeShutdownMetadata.Type type, Integer allocationDelaySeconds) {}

    private static String randomNodeId() {
        return UUID.randomUUID().toString();
    }

    private static ShardsAvailabilityHealthIndicatorService createAllocationHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new ShardsAvailabilityHealthIndicatorService(clusterService);
    }
}
