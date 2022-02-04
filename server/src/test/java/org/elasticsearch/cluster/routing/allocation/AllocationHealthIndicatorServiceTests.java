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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.AllocationHealthIndicatorService.NAME;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.health.HealthStatus.GREEN;
import static org.elasticsearch.health.HealthStatus.RED;
import static org.elasticsearch.health.HealthStatus.YELLOW;
import static org.elasticsearch.health.ServerHealthComponents.DATA;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AllocationHealthIndicatorServiceTests extends ESTestCase {

    public void testShouldBeGreenWhenActiveAndHasReplica() {
        var indices = randomList(1, 10, indexGenerator("green-index-", STARTED, STARTED));
        var clusterState = createClusterStateWith(indices);
        var service = createAllocationHealthIndicatorService(clusterState);

        assertThat(
            service.calculate(),
            equalTo(
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    GREEN,
                    "TODO 83240",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "green-shards-count",
                            indices.size(),
                            "yellow-shards-count",
                            0,
                            "yellow-shards",
                            List.of(),
                            "red-shards-count",
                            0,
                            "red-shards",
                            List.of()
                        )
                    )
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
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    YELLOW,
                    "TODO 83240",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "green-shards-count",
                            greenIndices.size(),
                            "yellow-shards-count",
                            1,
                            "yellow-shards",
                            List.of(yellowIndex.shards().get(1).shardId()),
                            "red-shards-count",
                            0,
                            "red-shards",
                            List.of()
                        )
                    )
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
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    YELLOW,
                    "TODO 83240",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "green-shards-count",
                            greenIndices.size(),
                            "yellow-shards-count",
                            1,
                            "yellow-shards",
                            List.of(yellowIndex.shards().get(1).shardId()),
                            "red-shards-count",
                            0,
                            "red-shards",
                            List.of()
                        )
                    )
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
                new HealthIndicatorResult(
                    NAME,
                    DATA,
                    RED,
                    "TODO 83240",
                    new SimpleHealthIndicatorDetails(
                        Map.of(
                            "green-shards-count",
                            greenIndices.size(),
                            "yellow-shards-count",
                            0,
                            "yellow-shards",
                            List.of(),
                            "red-shards-count",
                            1,
                            "red-shards",
                            List.of(redIndex.shards().get(1).shardId())
                        )
                    )
                )
            )
        );
    }

    private static ClusterState createClusterStateWith(List<IndexRoutingTable> indexes) {
        var builder = RoutingTable.builder();
        for (IndexRoutingTable index : indexes) {
            builder.add(index);
        }
        return ClusterState.builder(new ClusterName("test-cluster")).routingTable(builder.build()).build();
    }

    private static Supplier<IndexRoutingTable> indexGenerator(
        String prefix,
        ShardRoutingState primaryState,
        ShardRoutingState... replicaStates
    ) {
        var index = new AtomicInteger(0);
        return () -> index(prefix + index.incrementAndGet(), primaryState, replicaStates);
    }

    private static IndexRoutingTable index(String name, ShardRoutingState primaryState, ShardRoutingState... replicaStates) {
        var index = new Index(name, UUID.randomUUID().toString());
        var shardId = new ShardId(index, 1);

        var builder = IndexRoutingTable.builder(index);
        builder.addShard(createShardRouting(shardId, true, primaryState));
        for (ShardRoutingState replicaState : replicaStates) {
            builder.addShard(createShardRouting(shardId, false, replicaState));
        }
        return builder.build();
    }

    private static ShardRouting createShardRouting(ShardId shardId, boolean primary, ShardRoutingState state) {
        var routing = newUnassigned(
            shardId,
            primary,
            primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null)
        );
        if (state == ShardRoutingState.UNASSIGNED) {
            return routing;
        }
        routing = routing.initialize(UUID.randomUUID().toString(), null, 0);
        routing = routing.moveToStarted();
        if (state == STARTED) {
            return routing;
        }
        throw new AssertionError("Unexpected state [" + state + "]");
    }

    private static AllocationHealthIndicatorService createAllocationHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new AllocationHealthIndicatorService(clusterService);
    }
}
