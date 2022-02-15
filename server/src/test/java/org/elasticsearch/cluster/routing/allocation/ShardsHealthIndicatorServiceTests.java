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
import org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorService.ShardAllocationStats;
import org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorService.ShardHealthIndicatorDetails;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorService.NAME;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorServiceTests.ShardState.INITIALIZING_NEW;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorServiceTests.ShardState.STARTED;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorServiceTests.ShardState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorServiceTests.ShardState.UNASSIGNED_RESTARTING;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ShardsHealthIndicatorServiceTests extends ESTestCase {

    public void testShouldBeGreenWhenActiveAndHasReplica() {
        var indices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var clusterState = createClusterStateWith(indices);
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries and %d replicas.",
                        indices.size() * 2,
                        indices.size(),
                        indices.size()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = indices.size();
                        stats.allocatedReplicas = indices.size();
                    })
                )
            )
        );
    }

    public void testShouldBeYellowWhenInitializingFromScratch() {
        var indices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var initializing = index("initializing-index", INITIALIZING_NEW, INITIALIZING_NEW);
        var clusterState = createClusterStateWith(appendToCopy(indices, initializing));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries (%s initializing) and %d replicas (%s unallocated).",
                        (indices.size() + 1) * 2,
                        indices.size() + 1,
                        initializing.shards().get(1).shardId().toString(),
                        indices.size() + 1,
                        initializing.shards().get(1).shardId().toString()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = indices.size();
                        stats.allocatedReplicas = indices.size();
                        stats.initializingPrimaries.add(initializing.getShards().get(1).shardId());
                        stats.unallocatedReplicas.add(initializing.getShards().get(1).shardId());
                    })
                )
            )
        );
    }

    public void testShouldBeYellowWithNotActiveReplicas() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var yellowIndex = index("yellow-index", STARTED, UNASSIGNED);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, yellowIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries and %d replicas (%s unallocated).",
                        (greenIndices.size() + 1) * 2,
                        greenIndices.size() + 1,
                        greenIndices.size() + 1,
                        yellowIndex.shards().get(1).shardId().toString()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = greenIndices.size() + 1;
                        stats.allocatedReplicas = greenIndices.size();
                        stats.unallocatedReplicas.add(yellowIndex.getShards().get(1).shardId());
                    })
                )
            )
        );
    }

    public void testShouldBeYellowWithNoReplica() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var yellowIndex = index("yellow-index", STARTED);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, yellowIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    YELLOW,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries (%s unreplicated) and %d replicas.",
                        greenIndices.size() * 2 + 1,
                        greenIndices.size() + 1,
                        yellowIndex.shards().get(1).shardId().toString(),
                        greenIndices.size()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = greenIndices.size() + 1;
                        stats.allocatedReplicas = greenIndices.size();
                        stats.unreplicatedPrimaries.add(yellowIndex.getShards().get(1).shardId());
                    })
                )
            )
        );
    }

    public void testShouldBeRedWhenNotActive() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var redIndex = index("red-index", UNASSIGNED);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, redIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    RED,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries (%s unreplicated) (%s unallocated) and %d replicas.",
                        greenIndices.size() * 2 + 1,
                        greenIndices.size() + 1,
                        redIndex.shards().get(1).shardId().toString(),
                        redIndex.shards().get(1).shardId().toString(),
                        greenIndices.size()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = greenIndices.size();
                        stats.allocatedReplicas = greenIndices.size();
                        stats.unreplicatedPrimaries.add(redIndex.getShards().get(1).shardId());
                        stats.unallocatedPrimaries.add(redIndex.getShards().get(1).shardId());
                    })
                )
            )
        );
    }

    public void testShouldBeGreenIfReplicaUnavailableDueToRestarting() {
        var greenIndices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var restartingIndex = index("restarting-index", STARTED, UNASSIGNED_RESTARTING);
        var clusterState = createClusterStateWith(appendToCopy(greenIndices, restartingIndex));
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                createExpectedResult(
                    GREEN,
                    String.format(
                        Locale.ROOT,
                        "This cluster has %d shards including %d primaries and %d replicas (%s temporary unallocated, node is restarting).",
                        (greenIndices.size() + 1) * 2,
                        greenIndices.size() + 1,
                        greenIndices.size() + 1,
                        restartingIndex.shards().get(1).shardId().toString()
                    ),
                    createDetails(stats -> {
                        stats.allocatedPrimaries = greenIndices.size() + 1;
                        stats.allocatedReplicas = greenIndices.size();
                        stats.restartingReplicas.add(restartingIndex.getShards().get(1).shardId());
                    })
                )
            )
        );
    }

    private HealthIndicatorResult createExpectedResult(HealthStatus status, String summary, HealthIndicatorDetails details) {
        return new HealthIndicatorResult(NAME, DATA, status, summary, details);
    }

    private ShardHealthIndicatorDetails createDetails(Consumer<ShardAllocationStats> initializer) {
        var stats = new ShardAllocationStats();
        initializer.accept(stats);
        return new ShardHealthIndicatorDetails(stats);
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
            createRecoverySource(primary, state),
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        if (state == UNASSIGNED || state == INITIALIZING_NEW) {
            return routing;
        }
        routing = routing.initialize(UUID.randomUUID().toString(), null, 0);
        routing = routing.moveToStarted();
        if (state == STARTED) {
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
        if (state == UNASSIGNED_RESTARTING) {
            return routing;
        }

        throw new AssertionError("Unexpected state [" + state + "]");
    }

    private static RecoverySource createRecoverySource(boolean primary, ShardState state) {
        if (primary) {
            if (state == INITIALIZING_NEW) {
                return RecoverySource.EmptyStoreRecoverySource.INSTANCE;
            } else {
                return RecoverySource.ExistingStoreRecoverySource.INSTANCE;
            }
        } else {
            return RecoverySource.PeerRecoverySource.INSTANCE;
        }
    }

    public enum ShardState {
        UNASSIGNED,
        INITIALIZING_NEW,
        STARTED,
        UNASSIGNED_RESTARTING
    }

    private static ShardsHealthIndicatorService createAllocationHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new ShardsHealthIndicatorService(clusterService);
    }
}
