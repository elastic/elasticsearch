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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.AVAILABLE;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.RESTARTING;
import static org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorServiceTests.ShardState.UNAVAILABLE;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardsAvailabilityHealthIndicatorServiceTests extends ESTestCase {

    public void testShouldBeGreenWhenAllPrimariesAndReplicasAreStarted() {
        var replicatedIndices = randomList(1, 10, indexGenerator("replicated-index-", AVAILABLE, AVAILABLE));
        var unreplicatedIndices = randomList(1, 10, indexGenerator("unreplicated-index-", AVAILABLE));
        var clusterState = createClusterStateWith(concatLists(replicatedIndices, unreplicatedIndices));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has no unavailable shards.",
                    Map.of(
                        "started_primaries",
                        replicatedIndices.size() + unreplicatedIndices.size(),
                        "started_replicas",
                        replicatedIndices.size()
                    )
                )
            )
        );
    }

    public void testShouldBeYellowWhenThereAreUnassignedReplicas() {
        var availableReplicas = randomList(0, 5, () -> AVAILABLE);
        var unavailableReplicas = randomList(1, 5, () -> UNAVAILABLE);
        var yellowIndex = index("yellow-index-", AVAILABLE, concatLists(availableReplicas, unavailableReplicas).toArray(ShardState[]::new));
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", AVAILABLE, AVAILABLE));
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, yellowIndex));
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
                        greenIndices.size() + 1,
                        "unassigned_replicas",
                        unavailableReplicas.size(),
                        "started_replicas",
                        greenIndices.size() + availableReplicas.size()
                    )
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreUnassignedPrimaries() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", AVAILABLE, AVAILABLE));
        var redIndex = index("red-index-", UNAVAILABLE);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, redIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary.",
                    Map.of("unassigned_primaries", 1, "started_primaries", greenIndices.size(), "started_replicas", greenIndices.size())
                )
            )
        );
    }

    public void testShouldBeGreenWhenThereAreRestartingReplicas() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", AVAILABLE, AVAILABLE));
        var restartingIndex = index("restarting-index-", AVAILABLE, RESTARTING);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, restartingIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    "This cluster has 1 restarting replica.",
                    Map.of("started_primaries", greenIndices.size() + 1, "restarting_replicas", 1, "started_replicas", greenIndices.size())
                )
            )
        );
    }

    public void testShouldBeRedWhenThereAreRestartingPrimaries() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", AVAILABLE, AVAILABLE));
        var restartingIndex = index("restarting-index-", RESTARTING);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, restartingIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    "This cluster has 1 unavailable primary.",
                    Map.of("unassigned_primaries", 1, "started_primaries", greenIndices.size(), "started_replicas", greenIndices.size())
                )
            )
        );
    }

    private HealthIndicatorResult createExpectedResult(HealthStatus status, String summary, Map<String, Object> details) {
        return new HealthIndicatorResult(NAME, DATA, status, summary, new SimpleHealthIndicatorDetails(addDefaults(details)));
    }

    private static ClusterState createClusterStateWith(List<IndexRoutingTable> indexes) {
        var builder = RoutingTable.builder();
        for (IndexRoutingTable index : indexes) {
            builder.add(index);
        }
        return ClusterState.builder(new ClusterName("test-cluster"))
            .routingTable(builder.build())
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of())).build())
            .build();
    }

    private static Supplier<IndexRoutingTable> indexGenerator(String prefix, ShardState primaryState, ShardState... replicaStates) {
        var index = new AtomicInteger(0);
        return () -> index(prefix + index.incrementAndGet(), primaryState, replicaStates);
    }

    private static Map<String, Object> addDefaults(Map<String, Object> override) {
        return Map.of(
            "unassigned_primaries",
            override.getOrDefault("unassigned_primaries", 0),
            "initializing_primaries",
            override.getOrDefault("initializing_primaries", 0),
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

    private static IndexRoutingTable index(String name, ShardState primaryState, ShardState... replicaStates) {
        var index = new Index(name, UUID.randomUUID().toString());
        var shardId = new ShardId(index, 1);

        var builder = IndexRoutingTable.builder(index);
        builder.addShard(createShardRouting(shardId, true, primaryState));
        for (ShardState replicaState : replicaStates) {
            builder.addShard(createShardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static ShardRouting createShardRouting(ShardId shardId, boolean primary, ShardState state) {
        var routing = newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        if (state == UNAVAILABLE) {
            return routing;
        }
        routing = routing.initialize(UUID.randomUUID().toString(), null, 0);
        routing = routing.moveToStarted();
        if (state == AVAILABLE) {
            return routing;
        }
        routing = routing.moveToUnassigned(
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_RESTARTING,
                null,
                null,
                -1,
                0,
                0,
                false,
                UnassignedInfo.AllocationStatus.DELAYED_ALLOCATION,
                Set.of(),
                UUID.randomUUID().toString()
            )
        );
        if (state == RESTARTING) {
            return routing;
        }

        throw new AssertionError("Unexpected state [" + state + "]");
    }

    public enum ShardState {

        UNAVAILABLE,
        AVAILABLE,
        RESTARTING
    }

    private static ShardsAvailabilityHealthIndicatorService createAllocationHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new ShardsAvailabilityHealthIndicatorService(clusterService);
    }
}
