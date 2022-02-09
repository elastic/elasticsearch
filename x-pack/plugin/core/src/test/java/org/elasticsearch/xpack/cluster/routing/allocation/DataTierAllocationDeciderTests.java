/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import joptsimple.internal.Strings;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_COLD;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_FROZEN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DataTierAllocationDeciderTests extends ESAllocationTestCase {

    private static final DiscoveryNode HOT_NODE = newNode("node-hot", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
    private static final DiscoveryNode WARM_NODE = newNode("node-warm", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
    private static final DiscoveryNode COLD_NODE = newNode("node-cold", Collections.singleton(DiscoveryNodeRole.DATA_COLD_NODE_ROLE));
    private static final DiscoveryNode CONTENT_NODE = newNode(
        "node-content",
        Collections.singleton(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE)
    );
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            DataTierAllocationDecider.INSTANCE,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        )
    );
    private final AllocationService service = new AllocationService(
        allocationDeciders,
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE
    );

    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created")
    );

    public void testIndexPrefer() {
        ClusterState state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder().add(HOT_NODE).build())
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("myindex")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(DataTier.TIER_PREFERENCE, "data_warm,data_cold")
                                    .build()
                            )
                    )
                    .build()
            )
            .build();
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        allocation.debugDecision(true);
        Decision d;
        RoutingNode node;

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, COLD_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = DataTierAllocationDecider.INSTANCE.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(
                node.toString(),
                d.getExplanation(),
                containsString(
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                )
            );
            d = DataTierAllocationDecider.INSTANCE.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(
                node.toString(),
                d.getExplanation(),
                containsString(
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                )
            );
        }

        state = ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(DiscoveryNodes.builder().add(HOT_NODE).add(COLD_NODE).build())
            .metadata(
                Metadata.builder()
                    .put(
                        IndexMetadata.builder("myindex")
                            .settings(
                                Settings.builder()
                                    .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                    .put(IndexMetadata.SETTING_INDEX_UUID, "myindex")
                                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                    .put(DataTier.TIER_PREFERENCE, "data_warm,data_cold")
                                    .build()
                            )
                    )
                    .build()
            )
            .build();
        allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        allocation.debugDecision(true);

        for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
            node = new RoutingNode(n.getId(), n, shard);
            d = DataTierAllocationDecider.INSTANCE.canAllocate(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(
                node.toString(),
                d.getExplanation(),
                containsString(
                    "index has a preference for tiers [data_warm,data_cold] " + "and node does not meet the required [data_cold] tier"
                )
            );
            d = DataTierAllocationDecider.INSTANCE.canRemain(shard, node, allocation);
            assertThat(node.toString(), d.type(), equalTo(Decision.Type.NO));
            assertThat(
                node.toString(),
                d.getExplanation(),
                containsString(
                    "index has a preference for tiers [data_warm,data_cold] " + "and node does not meet the required [data_cold] tier"
                )
            );
        }

        node = new RoutingNode(COLD_NODE.getId(), COLD_NODE, shard);
        d = DataTierAllocationDecider.INSTANCE.canAllocate(shard, node, allocation);
        assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
        assertThat(
            node.toString(),
            d.getExplanation(),
            containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]")
        );
        d = DataTierAllocationDecider.INSTANCE.canRemain(shard, node, allocation);
        assertThat(node.toString(), d.type(), equalTo(Decision.Type.YES));
        assertThat(
            node.toString(),
            d.getExplanation(),
            containsString("index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]")
        );
    }

    public void testTierNodesPresent() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = DiscoveryNodes.builder().add(DATA_NODE).build();

        assertTrue(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));
    }

    public void testPreferredTierAvailable() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertThat(DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes), equalTo(Optional.empty()));
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes),
            equalTo(Optional.empty())
        );
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes),
            equalTo(Optional.empty())
        );
        assertThat(DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_cold"), nodes), equalTo(Optional.empty()));

        nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();

        assertThat(DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes), equalTo(Optional.empty()));
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes),
            equalTo(Optional.of("data_warm"))
        );
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes),
            equalTo(Optional.of("data_warm"))
        );
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_content,data_warm"), nodes),
            equalTo(Optional.of("data_content"))
        );
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_content,data_warm"), nodes),
            equalTo(Optional.of("data_content"))
        );
        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_cold,data_warm"), nodes),
            equalTo(Optional.of("data_warm"))
        );
    }

    public void testFrozenIllegalForRegularIndices() {
        List<String> tierList = new ArrayList<>(randomSubsetOf(DataTier.ALL_DATA_TIERS));
        if (tierList.contains(DATA_FROZEN) == false) {
            tierList.add(DATA_FROZEN);
        }
        Randomness.shuffle(tierList);

        String value = Strings.join(tierList, ",");
        Setting<String> setting = DataTier.TIER_PREFERENCE_SETTING;
        Settings.Builder builder = Settings.builder().put(setting.getKey(), value);
        if (randomBoolean()) {
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        }

        Settings settings = builder.build();
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(exception.getMessage(), equalTo("[data_frozen] tier can only be used for partial searchable snapshots"));
    }

    public void testFrozenLegalForPartialSnapshot() {
        Setting<String> setting = DataTier.TIER_PREFERENCE_SETTING;
        Settings.Builder builder = Settings.builder().put(setting.getKey(), DATA_FROZEN);
        builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        builder.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

        Settings settings = builder.build();

        // validate do not throw
        assertThat(setting.get(settings), equalTo(DATA_FROZEN));
    }

    public void testNonFrozenIllegalForPartialSnapshot() {
        List<String> tierList = new ArrayList<>(randomSubsetOf(DataTier.ALL_DATA_TIERS));
        if (tierList.contains(DATA_FROZEN)) {
            tierList.remove(DATA_FROZEN);
            tierList.add(DATA_COLD);
        }
        Randomness.shuffle(tierList);

        {
            String value = Strings.join(tierList, ",");
            Settings.Builder builder = Settings.builder().put(DataTier.TIER_PREFERENCE, value);
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
            builder.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

            Settings settings = builder.build();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataTier.TIER_PREFERENCE_SETTING.get(settings));
            assertThat(
                e.getMessage(),
                containsString("only the [data_frozen] tier preference may be used for partial searchable snapshots")
            );
        }

        {
            Settings.Builder builder = Settings.builder().put(DataTier.TIER_PREFERENCE, "");
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
            builder.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

            Settings settings = builder.build();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataTier.TIER_PREFERENCE_SETTING.get(settings));
            assertThat(
                e.getMessage(),
                containsString("only the [data_frozen] tier preference may be used for partial searchable snapshots")
            );
        }

        {
            Settings.Builder builder = Settings.builder().put(DataTier.TIER_PREFERENCE, "  ");
            builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
            builder.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

            Settings settings = builder.build();

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> DataTier.TIER_PREFERENCE_SETTING.get(settings));
            assertThat(
                e.getMessage(),
                containsString("only the [data_frozen] tier preference may be used for partial searchable snapshots")
            );
        }
    }

    public void testDefaultValueForPreference() {
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(Settings.EMPTY), equalTo(""));

        Settings.Builder builder = Settings.builder();
        builder.put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE);
        builder.put(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.getKey(), true);

        Settings settings = builder.build();
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(settings), equalTo(DATA_FROZEN));
    }
}
