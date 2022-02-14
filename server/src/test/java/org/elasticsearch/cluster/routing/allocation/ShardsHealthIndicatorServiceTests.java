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
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.SimpleHealthIndicatorDetails;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.routing.ShardRouting.newUnassigned;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.ShardsHealthIndicatorService.NAME;
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
                    createDetails(indices, indices, List.of(), List.of(), List.of())
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
                    createDetails(appendToCopy(greenIndices, yellowIndex), greenIndices, List.of(), List.of(yellowIndex), List.of())
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
                    createDetails(appendToCopy(greenIndices, yellowIndex), greenIndices, List.of(yellowIndex), List.of(), List.of())
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
                    createDetails(greenIndices, greenIndices, List.of(redIndex), List.of(), List.of(redIndex))
                )
            )
        );
    }

    private HealthIndicatorResult createExpectedResult(HealthStatus status, String summary, SimpleHealthIndicatorDetails details) {
        return new HealthIndicatorResult(NAME, DATA, status, summary, details);
    }

    private SimpleHealthIndicatorDetails createDetails(
        List<IndexRoutingTable> allocatedPrimaries,
        List<IndexRoutingTable> allocatedReplicas,
        List<IndexRoutingTable> unreplicatedPrimaries,
        List<IndexRoutingTable> unallocatedReplicas,
        List<IndexRoutingTable> unallocatedPrimaries
    ) {
        return new SimpleHealthIndicatorDetails(
            Map.of(
                "allocated_primaries_count",
                allocatedPrimaries.size(),
                "allocated_replicas_count",
                allocatedReplicas.size(),
                "unreplicated_primaries_count",
                unreplicatedPrimaries.size(),
                "unreplicated_primaries",
                unreplicatedPrimaries.stream().map(it -> it.shards().get(1).shardId()).toList(),
                "unallocated_replicas_count",
                unallocatedReplicas.size(),
                "unallocated_replicas",
                unallocatedReplicas.stream().map(it -> it.shards().get(1).shardId()).toList(),
                "unallocated_primaries_count",
                unallocatedPrimaries.size(),
                "unallocated_primaries",
                unallocatedPrimaries.stream().map(it -> it.shards().get(1).shardId()).toList()
            )
        );
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

    private static ShardsHealthIndicatorService createAllocationHealthIndicatorService(ClusterState clusterState) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new ShardsHealthIndicatorService(clusterService);
    }
}
