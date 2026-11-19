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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.Metadata.DEFAULT_PROJECT_ID;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.hamcrest.Matchers.equalTo;

/// The slots are only trusted to come from the data nodes when the settings of this node say so and the cluster state agrees. The tests
/// use real settings and cluster states, and the `RELOCATING` and `STARTED` shards on a shutting-down search node stand for a master that
/// has started all the relocations and one that has not, respectively.
public class TargetRelocationSlotsEstimatorTests extends ESTestCase {

    private static final String SOURCE = "source";
    private static final String PRIMARY_NODE = "primary-node";
    private static final String TARGET = "target";
    private static final String MASTER_NODE = "master-node";

    public void testSlotsAreTheRelocationsInFlightWhenTheDataNodeLimitIsNotRegistered() {
        // as on stateful, where the data node settings are not registered and so the limit is unbounded
        final var estimator = newEstimator(newClusterSettings(Settings.EMPTY, false), Settings.EMPTY);
        final var state = clusterState(RELOCATING, RELOCATING);

        final int inFlight = randomIntBetween(1, 20);
        assertThat(estimator.knownAvailableSlots(state, inFlight), equalTo(inFlight));
    }

    public void testSlotsAreTheDataNodeLimitWhenTheMasterDoesNotThrottleAndAllShardsRelocate() {
        final int limit = randomIntBetween(1, 10);
        final var estimator = newEstimator(newClusterSettings(dataNodeLimit(limit), true), Settings.EMPTY);
        final var state = clusterState(RELOCATING, RELOCATING);

        // the relocations in flight beyond the limit are queued on the target, and fewer of them do not reserve slots
        assertThat(estimator.knownAvailableSlots(state, 1), equalTo(limit));
        assertThat(estimator.knownAvailableSlots(state, limit + randomIntBetween(1, 10)), equalTo(limit));
    }

    public void testSlotsAreTheRelocationsInFlightUpToTheLimitWhenAShardHasNotStartedToRelocate() {
        final int limit = randomIntBetween(2, 10);
        final var estimator = newEstimator(newClusterSettings(dataNodeLimit(limit), true), Settings.EMPTY);
        // the settings may be ahead of the master's during a rolling upgrade, which would have started all the relocations otherwise
        final var state = clusterState(RELOCATING, STARTED);

        final int inFlight = randomIntBetween(1, limit - 1);
        assertThat(estimator.knownAvailableSlots(state, inFlight), equalTo(inFlight));
        assertThat(estimator.knownAvailableSlots(state, limit + randomIntBetween(1, 10)), equalTo(limit));
    }

    public void testSlotsAreTheRelocationsInFlightUpToTheLimitWhileTheMasterThrottles() {
        final int limit = randomIntBetween(2, 10);
        final var masterThrottles = randomFrom(
            ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
            ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING
        );
        final var settings = Settings.builder().put(dataNodeLimit(limit)).put(masterThrottles.getKey(), randomIntBetween(1, 5)).build();
        final var estimator = newEstimator(newClusterSettings(settings, true), Settings.EMPTY);
        final var state = clusterState(RELOCATING, RELOCATING);

        final int inFlight = randomIntBetween(1, limit - 1);
        assertThat(estimator.knownAvailableSlots(state, inFlight), equalTo(inFlight));
        assertThat(estimator.knownAvailableSlots(state, limit + randomIntBetween(1, 10)), equalTo(limit));
    }

    public void testRebalanceThrottlingByTheMasterIsNotTrustedToHaveBeenDisabled() {
        final int limit = randomIntBetween(2, 10);
        final var settings = Settings.builder()
            .put(dataNodeLimit(limit))
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), 2)
            .build();
        final var estimator = newEstimator(newClusterSettings(settings, true), Settings.EMPTY);

        assertThat(estimator.knownAvailableSlots(clusterState(RELOCATING, RELOCATING), 1), equalTo(1));
    }

    public void testSlotsFollowTheDynamicSettingUpdates() {
        final int limit = randomIntBetween(2, 10);
        final var clusterSettings = newClusterSettings(dataNodeLimit(limit), true);
        final var estimator = newEstimator(clusterSettings, Settings.EMPTY);
        final var state = clusterState(RELOCATING, RELOCATING);
        assertThat(estimator.knownAvailableSlots(state, 1), equalTo(limit));

        // the master starts to throttle
        clusterSettings.applySettings(
            Settings.builder()
                .put(dataNodeLimit(limit))
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 2)
                .build()
        );
        assertThat(estimator.knownAvailableSlots(state, 1), equalTo(1));

        // the master stops to throttle, and the limit changes
        final int newLimit = limit + randomIntBetween(1, 10);
        clusterSettings.applySettings(dataNodeLimit(newLimit));
        assertThat(estimator.knownAvailableSlots(state, 1), equalTo(newLimit));
    }

    private static TargetRelocationSlotsEstimator newEstimator(ClusterSettings clusterSettings, Settings nodeSettings) {
        return new TargetRelocationSlotsEstimator(clusterSettings, nodeSettings, ByteSizeValue.ofGb(1));
    }

    /// The master leaves the throttling of the relocations to the data nodes, as with data node recovery throttling, plus the data node
    /// limit on the incoming recoveries.
    private static Settings dataNodeLimit(int limit) {
        return Settings.builder()
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), limit)
            .build();
    }

    /// The master settings default to throttling the recoveries, unless the `settings` set them. The data node settings are registered
    /// only if `dataNodeSettingsRegistered`, as they only are by the stateless plugin.
    private static ClusterSettings newClusterSettings(Settings settings, boolean dataNodeSettingsRegistered) {
        final Set<Setting<?>> registeredSettings = new HashSet<>(
            List.of(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
                ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING
            )
        );
        if (dataNodeSettingsRegistered) {
            registeredSettings.addAll(DataNodeRecoveryThrottlingSettings.settings());
        }
        final var withMasterUnthrottled = Settings.builder()
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            .put(settings)
            .build();
        return new ClusterSettings(withMasterUnthrottled, registeredSettings);
    }

    /// Builds a cluster state where the search node [#SOURCE] is marked for removal and holds one search shard in each of the given
    /// states, which are `RELOCATING` to [#TARGET] or `STARTED`.
    private static ClusterState clusterState(ShardRoutingState... shardStates) {
        final var index = new Index("index", IndexMetadata.INDEX_UUID_NA_VALUE);
        final var indexMetadata = IndexMetadata.builder(index.getName())
            .settings(indexSettings(IndexVersion.current(), index.getUUID(), shardStates.length, 1))
            .build();
        final var routingBuilder = IndexRoutingTable.builder(index);
        for (int shard = 0; shard < shardStates.length; shard++) {
            final var shardId = new ShardId(index, shard);
            final var primary = TestShardRouting.shardRoutingBuilder(shardId, PRIMARY_NODE, true, STARTED)
                .withRole(ShardRouting.Role.INDEX_ONLY)
                .build();
            final var replicaBuilder = TestShardRouting.shardRoutingBuilder(shardId, SOURCE, false, shardStates[shard])
                .withRole(ShardRouting.Role.SEARCH_ONLY);
            if (shardStates[shard] == RELOCATING) {
                replicaBuilder.withRelocatingNodeId(TARGET);
            }
            routingBuilder.addIndexShard(new IndexShardRoutingTable.Builder(shardId).addShard(primary).addShard(replicaBuilder.build()));
        }

        final var shutdown = SingleNodeShutdownMetadata.builder()
            .setNodeId(SOURCE)
            .setType(SingleNodeShutdownMetadata.Type.REMOVE)
            .setReason("TargetRelocationSlotsEstimatorTests")
            .setStartedAtMillis(0L)
            .setNodeSeen(true)
            .build();
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(MASTER_NODE))
            .masterNodeId(MASTER_NODE)
            .localNodeId(MASTER_NODE)
            .add(DiscoveryNodeUtils.builder(PRIMARY_NODE).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(SOURCE).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(TARGET).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .build();
        return ClusterState.builder(new ClusterName("test"))
            .nodes(nodes)
            .metadata(
                Metadata.builder()
                    .putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(Map.of(SOURCE, shutdown)))
                    .put(ProjectMetadata.builder(DEFAULT_PROJECT_ID).put(indexMetadata, false))
                    .build()
            )
            .routingTable(GlobalRoutingTable.builder().put(DEFAULT_PROJECT_ID, RoutingTable.builder().add(routingBuilder).build()).build())
            .build();
    }
}
