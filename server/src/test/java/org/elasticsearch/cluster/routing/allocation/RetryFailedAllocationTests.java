/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class RetryFailedAllocationTests extends ESAllocationTestCase {

    private final MockAllocationService allocationService = createAllocationService();

    public void testRetryFailedResetsFailedAllocationsCounter() {

        final var inSyncId = UUIDs.randomBase64UUID();
        final var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 1, 1))
            .putInSyncAllocationIds(0, Set.of(inSyncId))
            .build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(newShardRouting(shardId, "node-1", null, true, STARTED, AllocationId.newInitializing(inSyncId)))
                            .addShard(newShardRouting(shardId, null, false, UNASSIGNED))
                    )
            )
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .build();
        clusterState = reroute(clusterState);

        Function<ClusterState, ShardRouting> replicaShard = state -> state.getRoutingTable().index(index).shard(0).replicaShards().get(0);

        // Exhaust all shard allocation attempts
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        for (int i = 1; i <= retries; i++) {
            clusterState = failShard(clusterState, replicaShard.apply(clusterState));
            clusterState = reroute(clusterState);
            assertShardStateAndAllocationFailures(replicaShard.apply(clusterState), i < retries ? INITIALIZING : UNASSIGNED, i);
        }
        // And extra reroute should not change cluster state as all retries exhausted
        assertThat(reroute(clusterState), equalTo(clusterState));

        // When counter is resetted
        clusterState = resetFailedCounters(clusterState);
        assertShardStateAndRelocationFailures(replicaShard.apply(clusterState), INITIALIZING, 0);
    }

    private static void assertShardStateAndAllocationFailures(ShardRouting shard, ShardRoutingState state, int failures) {
        assertThat(shard.state(), equalTo(state));
        assertThat(shard.unassignedInfo().getNumFailedAllocations(), equalTo(failures));
    }

    public void testRetryFailedResetsFailedRelocationsCounter() {

        final var inSyncId = UUIDs.randomBase64UUID();
        final var indexMetadata = IndexMetadata.builder("index")
            .settings(indexSettings(IndexVersion.current(), 1, 0).put("index.routing.allocation.require._id", "node-2"))
            .putInSyncAllocationIds(0, Set.of(inSyncId))
            .build();
        final var index = indexMetadata.getIndex();
        final var shardId = new ShardId(index, 0);

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(indexMetadata, false))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(index)
                            .addShard(newShardRouting(shardId, "node-1", null, true, STARTED, AllocationId.newInitializing(inSyncId)))
                    )
            )
            .nodes(DiscoveryNodes.builder().add(newNode("node-1")).add(newNode("node-2")))
            .build();
        clusterState = reroute(clusterState);

        Function<ClusterState, ShardRouting> sourceShard = state -> state.getRoutingNodes().node("node-1").getByShardId(shardId);
        Function<ClusterState, ShardRouting> targetShard = state -> state.getRoutingNodes().node("node-2").getByShardId(shardId);

        // Exhaust all shard relocation attempts
        final int retries = MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.get(Settings.EMPTY);
        for (int i = 1; i <= retries; i++) {
            clusterState = failShard(clusterState, targetShard.apply(clusterState));
            clusterState = reroute(clusterState);
            assertShardStateAndRelocationFailures(sourceShard.apply(clusterState), i < retries ? RELOCATING : STARTED, i);
        }
        // And extra reroute should not change cluster state as all retries exhausted
        assertThat(reroute(clusterState), equalTo(clusterState));

        // When counter is resetted
        clusterState = resetFailedCounters(clusterState);
        assertShardStateAndRelocationFailures(sourceShard.apply(clusterState), RELOCATING, 0);
    }

    private static void assertShardStateAndRelocationFailures(ShardRouting shard, ShardRoutingState state, int failures) {
        assertThat(shard.state(), equalTo(state));
        assertThat(shard.relocationFailureInfo().failedRelocations(), equalTo(failures));
    }

    private ClusterState resetFailedCounters(ClusterState clusterState) {
        return allocationService.reroute(clusterState, new AllocationCommands(), false, true, false, ActionListener.noop()).clusterState();
    }

    private ClusterState reroute(ClusterState clusterState) {
        return allocationService.reroute(clusterState, "test", ActionListener.noop());
    }

    private ClusterState failShard(ClusterState clusterState, ShardRouting shardRouting) {
        return allocationService.applyFailedShards(
            clusterState,
            List.of(new FailedShard(shardRouting, "simulated", new ElasticsearchException("simulated"), randomBoolean())),
            List.of()
        );
    }
}
