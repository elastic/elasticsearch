/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

public class SearchShardRelocationsTests extends ESTestCase {

    private static final String SOURCE_A = "source-a";
    private static final String SOURCE_B = "source-b";
    private static final String SOURCE_C = "source-c";
    private static final String TARGET = "target-node";
    private static final String OTHER_TARGET = "other-target-node";
    private static final String PRIMARY_NODE = "primary-node";
    private static final String INDEX_TARGET = "index-target-node";
    private static final String MASTER_NODE = "master-node";

    private record Relocation(String source, String target) {}

    /// A recovery that is not a relocation, of an unassigned shard, on the node.
    private record Recovery(String node, RecoveryPriority priority) {}

    public void testFromCountsOnlySearchShardsRelocatingBetweenTheTwoNodes() {
        final var state = clusterState(
            List.of(
                new Relocation(SOURCE_A, TARGET),
                new Relocation(SOURCE_A, TARGET),
                new Relocation(SOURCE_A, OTHER_TARGET),
                new Relocation(SOURCE_B, TARGET)
            ),
            // a shard that can be promoted to primary only lives on an index node, which is not a search node
            List.of(new Relocation(PRIMARY_NODE, INDEX_TARGET)),
            Map.of()
        );

        assertThat(SearchShardRelocations.countBetween(state, PRIMARY_NODE, TARGET), equalTo(0));
        assertThat(SearchShardRelocations.countBetween(state, PRIMARY_NODE, INDEX_TARGET), equalTo(0));
        assertThat(SearchShardRelocations.countBetween(state, SOURCE_A, TARGET), equalTo(2));
        assertThat(SearchShardRelocations.countBetween(state, SOURCE_A, OTHER_TARGET), equalTo(1));
        assertThat(SearchShardRelocations.countBetween(state, SOURCE_B, TARGET), equalTo(1));
        assertThat(SearchShardRelocations.countBetween(state, SOURCE_B, OTHER_TARGET), equalTo(0));
    }

    public void testFromIsZeroWhenThereIsNothingRelocatingOrTheNodeIsUnknown() {
        final var state = clusterState(List.of(new Relocation(SOURCE_A, TARGET)), List.of(), Map.of());

        assertThat(SearchShardRelocations.countBetween(state, SOURCE_C, TARGET), equalTo(0));
        assertThat(SearchShardRelocations.countBetween(state, "no-such-node", TARGET), equalTo(0));
        assertThat(SearchShardRelocations.countBetween(state, SOURCE_A, "no-such-node"), equalTo(0));
    }

    public void testFromShuttingDownNodesCountsOnlyNodesMarkedForRemoval() {
        final var removalType = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        final var state = clusterState(
            List.of(
                new Relocation(SOURCE_A, TARGET),
                new Relocation(SOURCE_A, TARGET),
                new Relocation(SOURCE_A, OTHER_TARGET),
                // restarting node is not marked for removal
                new Relocation(SOURCE_B, TARGET),
                // not shutting down
                new Relocation(SOURCE_C, TARGET)
            ),
            // an index node shutting down relocates its shards to an index node, which is not a search node
            List.of(new Relocation(PRIMARY_NODE, INDEX_TARGET)),
            Map.of(
                SOURCE_A,
                removalType,
                PRIMARY_NODE,
                SingleNodeShutdownMetadata.Type.REMOVE,
                SOURCE_B,
                SingleNodeShutdownMetadata.Type.RESTART,
                // stale shutdown metadata of a node that has left the cluster
                "gone-node",
                SingleNodeShutdownMetadata.Type.REMOVE
            )
        );

        assertThat(SearchShardRelocations.countFromShuttingDownNodesTo(state, TARGET), equalTo(2));
        assertThat(SearchShardRelocations.countFromShuttingDownNodesTo(state, OTHER_TARGET), equalTo(1));
        assertThat(SearchShardRelocations.countFromShuttingDownNodesTo(state, INDEX_TARGET), equalTo(0));
    }

    public void testFromShuttingDownNodesSumsAcrossShuttingDownNodes() {
        final var state = clusterState(
            List.of(new Relocation(SOURCE_A, TARGET), new Relocation(SOURCE_B, TARGET), new Relocation(SOURCE_B, TARGET)),
            List.of(),
            Map.of(SOURCE_A, SingleNodeShutdownMetadata.Type.REMOVE, SOURCE_B, SingleNodeShutdownMetadata.Type.REMOVE)
        );

        assertThat(SearchShardRelocations.countFromShuttingDownNodesTo(state, TARGET), equalTo(3));
        assertThat(SearchShardRelocations.countFromShuttingDownNodesTo(state, OTHER_TARGET), equalTo(0));
    }

    public void testNonCriticalRecoveriesDoNotIncludeTheCriticalOnes() {
        final var state = clusterState(
            List.of(
                new Relocation(SOURCE_A, TARGET),
                new Relocation(SOURCE_A, OTHER_TARGET),
                new Relocation(SOURCE_B, TARGET),
                new Relocation(SOURCE_B, TARGET),
                new Relocation(SOURCE_C, TARGET),
                new Relocation(SOURCE_C, OTHER_TARGET)
            ),
            List.of(),
            Map.of(),
            List.of(),
            List.of(
                new Recovery(TARGET, RecoveryPriority.UNASSIGNED_PERFORMANCE_SCALING),
                new Recovery(TARGET, RecoveryPriority.UNASSIGNED_UNEXPECTED),
                new Recovery(OTHER_TARGET, RecoveryPriority.UNASSIGNED_RESTORING),
                new Recovery(OTHER_TARGET, RecoveryPriority.UNASSIGNED_PERFORMANCE_SCALING)
            )
        );

        // 4 relocations and 1 other non-critical recovery, the critical recovery is not counted
        assertThat(SearchShardRelocations.countNonCriticalInitializing(state, TARGET), equalTo(5));
        // 2 relocations and 2 other non-critical recoveries
        assertThat(SearchShardRelocations.countNonCriticalInitializing(state, OTHER_TARGET), equalTo(4));
        assertThat(SearchShardRelocations.countNonCriticalInitializing(state, SOURCE_A), equalTo(0));
        assertThat(SearchShardRelocations.countNonCriticalInitializing(state, "no-such-node"), equalTo(0));
    }

    public void testPendingRelocationsCountsOnlyTheStartedSearchShardsOfTheNode() {
        final var state = clusterState(
            // a relocating shard is not pending, it is already counted as a relocation
            List.of(new Relocation(SOURCE_A, TARGET)),
            // a shard that can be promoted to primary only lives on an index node, which is not a search node
            List.of(new Relocation(PRIMARY_NODE, INDEX_TARGET)),
            Map.of(),
            List.of(SOURCE_A, SOURCE_A, SOURCE_B)
        );

        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, SOURCE_A), equalTo(2));
        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, SOURCE_B), equalTo(1));
        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, SOURCE_C), equalTo(0));
        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, TARGET), equalTo(0));
        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, PRIMARY_NODE), equalTo(0));
        assertThat(SearchShardRelocations.countPendingRelocationsOn(state, "no-such-node"), equalTo(0));
    }

    public void testHasPendingRelocationsOnlyWhileANodeMarkedForRemovalHoldsStartedSearchShards() {
        final var removalType = randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.SIGTERM);
        final var relocatingOnly = clusterState(List.of(new Relocation(SOURCE_A, TARGET)), List.of(), Map.of(SOURCE_A, removalType));
        assertThat(SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(relocatingOnly), equalTo(false));

        final var startedOnNodeMarkedForRemoval = clusterState(
            List.of(new Relocation(SOURCE_A, TARGET)),
            List.of(),
            Map.of(SOURCE_A, removalType),
            List.of(SOURCE_A)
        );
        assertThat(SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(startedOnNodeMarkedForRemoval), equalTo(true));

        // a restarting node is not marked for removal, and a node that is not shutting down is not waited for
        final var startedOnOtherNodes = clusterState(
            List.of(),
            List.of(),
            Map.of(SOURCE_B, SingleNodeShutdownMetadata.Type.RESTART),
            List.of(SOURCE_B, SOURCE_C)
        );
        assertThat(SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(startedOnOtherNodes), equalTo(false));

        // stale shutdown metadata of a node that has left the cluster
        final var staleShutdown = clusterState(List.of(), List.of(), Map.of("gone-node", removalType), List.of(SOURCE_C));
        assertThat(SearchShardRelocations.hasPendingRelocationsOnShuttingDownNodes(staleShutdown), equalTo(false));
    }

    private static DiscoveryNode searchNode(String nodeId) {
        return DiscoveryNodeUtils.builder(nodeId).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
    }

    /// Builds a cluster state with one search replica, relocating from `source` to `target`, per entry of `searchRelocations`, and one
    /// primary that can be promoted, relocating from `source` to `target`, per entry of `promotableRelocations`. The nodes in
    /// `shutdowns` are marked for shutdown of the given type, whether they are in the cluster or not.
    private static ClusterState clusterState(
        List<Relocation> searchRelocations,
        List<Relocation> promotableRelocations,
        Map<String, SingleNodeShutdownMetadata.Type> shutdowns
    ) {
        return clusterState(searchRelocations, promotableRelocations, shutdowns, List.of());
    }

    /// As above, with one more search replica started on the node, per entry of `startedSearchShardNodes`.
    private static ClusterState clusterState(
        List<Relocation> searchRelocations,
        List<Relocation> promotableRelocations,
        Map<String, SingleNodeShutdownMetadata.Type> shutdowns,
        List<String> startedSearchShardNodes
    ) {
        return clusterState(searchRelocations, promotableRelocations, shutdowns, startedSearchShardNodes, List.of());
    }

    /// As above, with one more search replica initializing on the node, without a relocation, per entry of `initializingRecoveries`.
    private static ClusterState clusterState(
        List<Relocation> searchRelocations,
        List<Relocation> promotableRelocations,
        Map<String, SingleNodeShutdownMetadata.Type> shutdowns,
        List<String> startedSearchShardNodes,
        List<Recovery> initializingRecoveries
    ) {
        final int numShards = searchRelocations.size() + promotableRelocations.size() + startedSearchShardNodes.size()
            + initializingRecoveries.size();
        final var index = new Index("index", IndexMetadata.INDEX_UUID_NA_VALUE);
        final var indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), numShards, 1))
            .build();
        final var routingBuilder = IndexRoutingTable.builder(index);
        int shard = 0;
        for (final var relocation : searchRelocations) {
            final var shardId = new ShardId(index, shard++);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, PRIMARY_NODE, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final var replica = TestShardRouting.shardRoutingBuilder(shardId, relocation.source(), false, RELOCATING)
                .withRelocatingNodeId(relocation.target())
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica));
        }
        for (final var relocation : promotableRelocations) {
            final var shardId = new ShardId(index, shard++);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, relocation.source(), true, RELOCATING)
                .withRelocatingNodeId(relocation.target())
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary));
        }
        for (final var nodeId : startedSearchShardNodes) {
            final var shardId = new ShardId(index, shard++);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, PRIMARY_NODE, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final var replica = TestShardRouting.shardRoutingBuilder(shardId, nodeId, false, STARTED)
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica));
        }
        for (final var recovery : initializingRecoveries) {
            final var shardId = new ShardId(index, shard++);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, PRIMARY_NODE, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final var replica = TestShardRouting.shardRoutingBuilder(shardId, recovery.node(), false, INITIALIZING)
                .withRole(ShardRouting.Role.SEARCH_ONLY)
                .withRecoveryPriority(recovery.priority())
                .build();
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replica));
        }

        final var shutdownMetadata = new HashMap<String, SingleNodeShutdownMetadata>();
        shutdowns.forEach((nodeId, type) -> {
            shutdownMetadata.put(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setType(type)
                    .setReason("SearchShardRelocationCountsTests")
                    .setStartedAtMillis(0L)
                    .setNodeSeen(true)
                    .setGracePeriod(type == SingleNodeShutdownMetadata.Type.SIGTERM ? TimeValue.timeValueSeconds(30) : null)
                    .build()
            );
        });

        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(MASTER_NODE))
            .masterNodeId(MASTER_NODE)
            .localNodeId(MASTER_NODE)
            .add(DiscoveryNodeUtils.builder(PRIMARY_NODE).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(INDEX_TARGET).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(searchNode(SOURCE_A))
            .add(searchNode(SOURCE_B))
            .add(searchNode(SOURCE_C))
            .add(searchNode(TARGET))
            .add(searchNode(OTHER_TARGET))
            .build();
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownMetadata))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build())
            .build();
    }
}
