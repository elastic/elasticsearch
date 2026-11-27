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
import org.elasticsearch.core.Nullable;
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
    /// The node that the relocations of the cluster states go to, unless they are said to go elsewhere.
    static final String RELOCATION_TARGET_NODE = "target-node";
    /// Another node that relocations go to, which is not the target of the shard under test.
    private static final String OTHER_TARGET_NODE = "other-target-node";

    private OtherNodeShutdownClusterStates() {}

    /// A node marked for REMOVE since `startedAtMillis`, with `relocatingShards` search shards relocating from it to the target,
    /// `pendingShards` search shards still STARTED on it, which the master has not begun to relocate, and `elsewhereShards` search shards
    /// relocating from it to another node than the target.
    record ShuttingDownNode(String nodeId, int relocatingShards, int pendingShards, int elsewhereShards, long startedAtMillis) {

        ShuttingDownNode(String nodeId, int relocatingShards, int pendingShards, long startedAtMillis) {
            this(nodeId, relocatingShards, pendingShards, 0, startedAtMillis);
        }

        ShuttingDownNode(String nodeId, int relocatingShards, long startedAtMillis) {
            this(nodeId, relocatingShards, 0, startedAtMillis);
        }

        /// A node whose `shards` search shards all relocate to another node than the target, so none of them uses the target's slots.
        static ShuttingDownNode relocatingElsewhere(String nodeId, int shards, long startedAtMillis) {
            return new ShuttingDownNode(nodeId, 0, 0, shards, startedAtMillis);
        }
    }

    /// The search replica of a shard of the index: the node it is on, and, if it is `RELOCATING`, the node it goes to, which is
    /// [#RELOCATION_TARGET_NODE] unless it is said to go elsewhere.
    private record SearchShardSpec(String nodeId, ShardRoutingState state, @Nullable RecoveryPriority priority, String relocatingToNodeId) {

        SearchShardSpec(String nodeId, ShardRoutingState state, @Nullable RecoveryPriority priority) {
            this(nodeId, state, priority, RELOCATION_TARGET_NODE);
        }

        SearchShardSpec(String nodeId, ShardRoutingState state) {
            this(nodeId, state, null);
        }
    }

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

    /// Like [#relocationWithOtherNodeShuttingDown], with `otherShardsFromSource` more search shards relocating from `healthy-source` to
    /// the target, and with `healthy-source` marked for REMOVE since `sourceShutdownStartedAtMillis` unless that is `null`, as when the
    /// source starts shutting down while the shard is recovering.
    public static ClusterState relocationFromSourceWithOtherNodeShuttingDown(
        int otherShardsFromSource,
        @Nullable Long sourceShutdownStartedAtMillis,
        int relocatingShards,
        Index index,
        long shutdownStartedAtMillis
    ) {
        return relocationWithOtherNodesShuttingDown(
            index,
            List.of(new ShuttingDownNode("shutting-node", relocatingShards, shutdownStartedAtMillis)),
            0,
            0,
            true,
            otherShardsFromSource,
            sourceShutdownStartedAtMillis
        );
    }

    /// Like [#relocationWithOtherNodeShuttingDown], with several shutting-down nodes, which may have different deadlines.
    static ClusterState relocationWithOtherNodesShuttingDown(Index index, List<ShuttingDownNode> shuttingDownNodes) {
        return relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 0);
    }

    /// As above, with `otherRecoveriesOnTarget` more shards initializing on the target, which are not relocations, as the non-critical
    /// recoveries of unassigned shards.
    static ClusterState relocationWithOtherNodesShuttingDown(
        Index index,
        List<ShuttingDownNode> shuttingDownNodes,
        int otherRecoveriesOnTarget
    ) {
        return relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, otherRecoveriesOnTarget, 0);
    }

    /// As above, with `criticalRecoveriesOnTarget` more shards initializing on the target, which are critical recoveries of unassigned
    /// shards.
    static ClusterState relocationWithOtherNodesShuttingDown(
        Index index,
        List<ShuttingDownNode> shuttingDownNodes,
        int otherRecoveriesOnTarget,
        int criticalRecoveriesOnTarget
    ) {
        return relocationWithOtherNodesShuttingDown(
            index,
            shuttingDownNodes,
            otherRecoveriesOnTarget,
            criticalRecoveriesOnTarget,
            true,
            0,
            null
        );
    }

    /// The cluster state of [#relocationWithOtherNodesShuttingDown] once the master has cancelled the relocation of the shard under
    /// test: the shard is not among the recoveries on the target any more, though the routing the shard was recovering with still showed
    /// it relocating there.
    static ClusterState relocationCancelledWithOtherNodesShuttingDown(Index index, List<ShuttingDownNode> shuttingDownNodes) {
        return relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 0, 0, false, 0, null);
    }

    private static ClusterState relocationWithOtherNodesShuttingDown(
        Index index,
        List<ShuttingDownNode> shuttingDownNodes,
        int otherRecoveriesOnTarget,
        int criticalRecoveriesOnTarget,
        boolean shardUnderTestRelocating,
        int otherShardsFromSource,
        @Nullable Long sourceShutdownStartedAtMillis
    ) {
        // shard 0 is the one under test, the other shards of its source, and the relocating and the pending shards of each shutting-down
        // node follow it
        final var searchShards = new ArrayList<SearchShardSpec>();
        if (shardUnderTestRelocating) {
            searchShards.add(new SearchShardSpec(RELOCATION_SOURCE_NODE, RELOCATING));
        }
        searchShards.addAll(Collections.nCopies(otherShardsFromSource, new SearchShardSpec(RELOCATION_SOURCE_NODE, RELOCATING)));
        for (final var shuttingDownNode : shuttingDownNodes) {
            searchShards.addAll(
                Collections.nCopies(shuttingDownNode.relocatingShards(), new SearchShardSpec(shuttingDownNode.nodeId(), RELOCATING))
            );
            searchShards.addAll(
                Collections.nCopies(shuttingDownNode.pendingShards(), new SearchShardSpec(shuttingDownNode.nodeId(), STARTED))
            );
            searchShards.addAll(
                Collections.nCopies(
                    shuttingDownNode.elsewhereShards(),
                    new SearchShardSpec(shuttingDownNode.nodeId(), RELOCATING, null, OTHER_TARGET_NODE)
                )
            );
        }
        searchShards.addAll(
            Collections.nCopies(
                otherRecoveriesOnTarget,
                new SearchShardSpec(RELOCATION_TARGET_NODE, INITIALIZING, RecoveryPriority.UNASSIGNED_PERFORMANCE_SCALING)
            )
        );
        searchShards.addAll(
            Collections.nCopies(
                criticalRecoveriesOnTarget,
                new SearchShardSpec(RELOCATION_TARGET_NODE, INITIALIZING, RecoveryPriority.UNASSIGNED_UNEXPECTED)
            )
        );
        // the source is already a node of the cluster state, so it only needs the shutdown metadata
        final var shutdowns = new ArrayList<>(shuttingDownNodes);
        if (sourceShutdownStartedAtMillis != null) {
            shutdowns.add(new ShuttingDownNode(RELOCATION_SOURCE_NODE, 0, sourceShutdownStartedAtMillis));
        }
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes(shuttingDownNodes))
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata(shutdowns))
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
                replicaBuilder.withRelocatingNodeId(searchShard.relocatingToNodeId());
            }
            if (searchShard.priority() != null) {
                // the builder would pick the priority at random
                replicaBuilder.withRecoveryPriority(searchShard.priority());
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
            .add(DiscoveryNodeUtils.create(RELOCATION_TARGET_NODE))
            .add(DiscoveryNodeUtils.create(OTHER_TARGET_NODE));
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
