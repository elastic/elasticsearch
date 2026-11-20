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
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoveryPriority;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.test.ESTestCase.indexSettings;

/// The cluster states and the settings for the tests of the warming timeouts of a search shard that relocates from a source that is not
/// shutting down while other nodes are, see [RelocationOtherNodeShutdownTimeouts].
public final class OtherNodeShutdownClusterStates {

    private static final String MASTER_NODE = "master-node";
    private static final String PRIMARY_NODE = "primary-node";
    /// The node that the shard under test relocates from, which is not shutting down.
    private static final String RELOCATION_SOURCE_NODE = "healthy-source";
    /// The node that all the relocations of the cluster states go to.
    static final String RELOCATION_TARGET_NODE = "target-node";

    private OtherNodeShutdownClusterStates() {}

    /// A node marked for REMOVE since `startedAtMillis`, with `relocatingShards` search shards relocating from it to the target, and
    /// `pendingShards` search shards still STARTED on it, which the master has not begun to relocate.
    record ShuttingDownNode(String nodeId, int relocatingShards, int pendingShards, long startedAtMillis) {

        ShuttingDownNode(String nodeId, int relocatingShards, long startedAtMillis) {
            this(nodeId, relocatingShards, 0, startedAtMillis);
        }
    }

    /// The search replica of a shard of the index: the node it is on, and, if it is `RELOCATING`, it goes to [#RELOCATION_TARGET_NODE].
    private record SearchShardSpec(String nodeId, ShardRoutingState state) {}

    /// The settings with which the shard shares the target's slots with the shards of the shutting-down nodes: a grace cap of 10s, the
    /// sharing enabled, and the data nodes accepting 2 relocations at once.
    public static Settings slotSharingSettings() {
        return Settings.builder()
            .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), "10s")
            .put(SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING.getKey(), true)
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 2)
            .build();
    }

    /// Builds a cluster state where shard 0 relocates from `healthy-source`, which is not shutting down, to `target-node`,
    /// while `relocatingShards` other search shards relocate to the same target from the shutting-down `shutting-node`, which is marked
    /// for REMOVE since `shutdownStartedAtMillis`.
    public static ClusterState relocationWithOtherNodeShuttingDown(int relocatingShards, Index index, long shutdownStartedAtMillis) {
        return relocationWithOtherNodesShuttingDown(
            index,
            List.of(new ShuttingDownNode("shutting-node", relocatingShards, shutdownStartedAtMillis))
        );
    }

    /// Like [#relocationWithOtherNodeShuttingDown], with several shutting-down nodes, which may have different deadlines.
    static ClusterState relocationWithOtherNodesShuttingDown(Index index, List<ShuttingDownNode> shuttingDownNodes) {
        return relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 0);
    }

    /// As above, with `otherRecoveriesOnTarget` more shards initializing on the target, which are not relocations, as the recoveries of
    /// unassigned shards.
    static ClusterState relocationWithOtherNodesShuttingDown(
        Index index,
        List<ShuttingDownNode> shuttingDownNodes,
        int otherRecoveriesOnTarget
    ) {
        // shard 0 is the one under test, the relocating and the pending shards of each shutting-down node follow it
        final var searchShards = new ArrayList<SearchShardSpec>();
        searchShards.add(new SearchShardSpec(RELOCATION_SOURCE_NODE, RELOCATING));
        for (final var shuttingDownNode : shuttingDownNodes) {
            searchShards.addAll(
                Collections.nCopies(shuttingDownNode.relocatingShards(), new SearchShardSpec(shuttingDownNode.nodeId(), RELOCATING))
            );
            searchShards.addAll(
                Collections.nCopies(shuttingDownNode.pendingShards(), new SearchShardSpec(shuttingDownNode.nodeId(), STARTED))
            );
        }
        searchShards.addAll(Collections.nCopies(otherRecoveriesOnTarget, new SearchShardSpec(RELOCATION_TARGET_NODE, INITIALIZING)));
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes(shuttingDownNodes))
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(shuttingDownNodes))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata(index, searchShards.size()), false))
                    .build()
            )
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routing(index, searchShards)).build())
                    .build()
            )
            .build();
    }

    /// The target copy of the relocation that is under test in the cluster states above.
    public static ShardRouting selfOfRelocation(ClusterState state, Index index) {
        return state.routingTable(DEFAULT_PROJECT_ID)
            .shardRoutingTable(new ShardId(index, 0))
            .shardsWithState(RELOCATING)
            .stream()
            .filter(shard -> shard.primary() == false)
            .findFirst()
            .orElseThrow()
            .getTargetRelocatingShard();
    }

    private static IndexMetadata indexMetadata(Index index, int numShards) {
        return IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), numShards, 1))
            .build();
    }

    /// One shard per entry of `searchShards`, each with an `INDEX_ONLY` primary started on [#PRIMARY_NODE] and the described search
    /// replica.
    private static IndexRoutingTable.Builder routing(Index index, List<SearchShardSpec> searchShards) {
        final var routingBuilder = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < searchShards.size(); shard++) {
            final var shardId = new ShardId(index, shard);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, PRIMARY_NODE, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final var searchShard = searchShards.get(shard);
            final var replicaBuilder = TestShardRouting.shardRoutingBuilder(shardId, searchShard.nodeId(), false, searchShard.state())
                .withRole(ShardRouting.Role.SEARCH_ONLY);
            if (searchShard.state() == RELOCATING) {
                replicaBuilder.withRelocatingNodeId(RELOCATION_TARGET_NODE);
            } else if (searchShard.state() == INITIALIZING) {
                // a non-critical recovery, which uses the non-critical slots of the target, as the builder would pick the priority at
                // random
                replicaBuilder.withRecoveryPriority(RecoveryPriority.UNASSIGNED_PERFORMANCE_SCALING);
            }
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replicaBuilder.build()));
        }
        return routingBuilder;
    }

    private static DiscoveryNodes nodes(List<ShuttingDownNode> shuttingDownNodes) {
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(MASTER_NODE))
            .masterNodeId(MASTER_NODE)
            .localNodeId(MASTER_NODE)
            .add(DiscoveryNodeUtils.create(PRIMARY_NODE))
            .add(DiscoveryNodeUtils.create(RELOCATION_SOURCE_NODE))
            .add(DiscoveryNodeUtils.create(RELOCATION_TARGET_NODE));
        shuttingDownNodes.forEach(shuttingDownNode -> nodes.add(DiscoveryNodeUtils.create(shuttingDownNode.nodeId())));
        return nodes.build();
    }

    private static NodesShutdownMetadata shutdownMetadata(List<ShuttingDownNode> shuttingDownNodes) {
        return new NodesShutdownMetadata(
            shuttingDownNodes.stream()
                .collect(
                    toMap(
                        ShuttingDownNode::nodeId,
                        shuttingDownNode -> SingleNodeShutdownMetadata.builder()
                            .setNodeId(shuttingDownNode.nodeId())
                            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
                            .setReason("OtherNodeShutdownClusterStates")
                            .setStartedAtMillis(shuttingDownNode.startedAtMillis())
                            .setNodeSeen(true)
                            .build(),
                        (a, b) -> b
                    )
                )
        );
    }
}
