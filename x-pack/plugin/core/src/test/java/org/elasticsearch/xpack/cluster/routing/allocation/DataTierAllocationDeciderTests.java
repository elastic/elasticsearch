/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import joptsimple.internal.Strings;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_COLD;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_FROZEN;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
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

    private static final DesiredNode HOT_DESIRED_NODE = newDesiredNode("node-hot", DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
    private static final DesiredNode WARM_DESIRED_NODE = newDesiredNode("node-warm", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
    private static final DesiredNode COLD_DESIRED_NODE = newDesiredNode("node-cold", DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
    private static final DesiredNode CONTENT_DESIRED_NODE = newDesiredNode("node-content", DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE);
    private static final DesiredNode DATA_DESIRED_NODE = newDesiredNode("node-data", DiscoveryNodeRole.DATA_ROLE);

    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            DataTierAllocationDecider.INSTANCE,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        )
    );

    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created")
    );

    public void testIndexPrefer() {
        {
            final var desiredNodes = randomBoolean() ? null : createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE);
            final var clusterState = clusterStateWithIndexAndNodes(
                "data_warm,data_cold",
                DiscoveryNodes.builder().add(HOT_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                );
            }
        }

        {
            final var desiredNodes = randomBoolean() ? null : createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, COLD_DESIRED_NODE);
            final var clusterState = clusterStateWithIndexAndNodes(
                "data_warm,data_cold",
                DiscoveryNodes.builder().add(HOT_NODE).add(COLD_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold] and node does not meet the required [data_cold] tier"
                );
            }

            assertAllocationDecision(
                clusterState,
                COLD_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]"
            );
        }

        {
            // Remove the cold tier from desired nodes
            final var desiredNodes = createDesiredNodesWithActualizedNodes(WARM_DESIRED_NODE);
            final var state = clusterStateWithIndexAndNodes(
                "data_cold,data_warm",
                DiscoveryNodes.builder().add(WARM_NODE).add(COLD_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode node : List.of(HOT_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    state,
                    node,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_cold,data_warm] and node does not meet the required [data_warm] tier"
                );
            }

            assertAllocationDecision(
                state,
                WARM_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_cold,data_warm] and node has tier [data_warm]"
            );
        }

        {
            // There's a warm node in the desired nodes, but it hasn't joined the cluster yet,
            // in that case we consider that there aren't any nodes with the preferred tier in the cluster
            final ClusterState clusterState;
            final String tierPreference;
            if (randomBoolean()) {
                tierPreference = "data_warm,data_cold";
                clusterState = clusterStateWithIndexAndNodes(
                    tierPreference,
                    DiscoveryNodes.builder().add(HOT_NODE).build(),
                    DesiredNodes.create("history", 1, List.of(pendingDesiredNode(WARM_DESIRED_NODE)))
                );
            } else {
                tierPreference = "data_warm,data_hot";
                clusterState = clusterStateWithIndexAndNodes(
                    tierPreference,
                    DiscoveryNodes.builder().add(COLD_NODE).build(),
                    DesiredNodes.create("history", 1, List.of(pendingDesiredNode(WARM_DESIRED_NODE)))
                );
            }

            for (DiscoveryNode node : List.of(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    node,
                    Decision.Type.NO,
                    String.format(
                        Locale.ROOT,
                        "index has a preference for tiers [%s], but no nodes for any of those tiers are available in the cluster",
                        tierPreference
                    )
                );
            }

        }
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

    public void testTierNodesPresentDesiredNodes() {
        Set<DesiredNode> nodes = Collections.emptySet();

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = Set.of(WARM_DESIRED_NODE, CONTENT_DESIRED_NODE);

        assertFalse(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertFalse(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));

        nodes = Set.of(DATA_DESIRED_NODE);

        assertTrue(DataTierAllocationDecider.tierNodesPresent("data", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_hot", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_warm", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_cold", nodes));
        assertTrue(DataTierAllocationDecider.tierNodesPresent("data_content", nodes));
    }

    public void testPreferredTierAvailable() {
        {
            final var nodes = DiscoveryNodes.builder().build();
            final DesiredNodes desiredNodes = randomBoolean()
                ? null
                : createDesiredNodesWithPendingNodes(HOT_DESIRED_NODE, WARM_DESIRED_NODE, COLD_DESIRED_NODE);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_cold"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
        }

        {
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            final var desiredNodes = randomBoolean()
                ? null
                : createDesiredNodesWithActualizedNodes(WARM_DESIRED_NODE, CONTENT_DESIRED_NODE);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_content,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            final var desiredNodes = createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, WARM_DESIRED_NODE, CONTENT_DESIRED_NODE);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_hot"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_content,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_hot"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_hot"))
            );
        }

        {
            // When there are desired nodes that haven't joined the cluster yet, those are not considered
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            // i.e. HOT_DESIRED_NODE might be part of the DesiredNodes, but it is not part of the cluster yet
            final var desiredNodes = DesiredNodes.create(
                randomAlphaOfLength(10),
                1,
                List.of(
                    pendingDesiredNode(HOT_DESIRED_NODE),
                    actualizedDesiredNode(WARM_DESIRED_NODE),
                    actualizedDesiredNode(CONTENT_DESIRED_NODE)
                )
            );

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_warm,data_content"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_content,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            // Cold tier is planned to be removed
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final var desiredNodes = createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, WARM_DESIRED_NODE);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_cold,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            // During grow and shrink (i.e. a way to replace a node) we should avoid moving the shard from a preferred tier to a less
            // preferred tier if there's a node that can hold that shard and we know that a new desired node would substitute the old one
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final var desiredNodes = DesiredNodes.create(
                "history",
                1,
                List.of(
                    actualizedDesiredNode(HOT_DESIRED_NODE),
                    actualizedDesiredNode(WARM_DESIRED_NODE),
                    pendingDesiredNode(COLD_DESIRED_NODE)
                )
            );

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_cold,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_cold"))
            );
        }

        {
            // Ensure that when we are removing a tier and growing the next preferred tier we wait until all the new
            // nodes have joined the cluster avoiding filling the new nodes with shards from the removed tier
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final DesiredNodes desiredNodes;
            // Grow any of the next preferred tiers
            if (randomBoolean()) {
                final var newWarmNode = newDesiredNode("node-warm-2", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
                desiredNodes = DesiredNodes.create(
                    "history",
                    1,
                    List.of(
                        actualizedDesiredNode(HOT_DESIRED_NODE),
                        actualizedDesiredNode(WARM_DESIRED_NODE),
                        pendingDesiredNode(newWarmNode)
                    )
                );
            } else {
                final var newHotNode = newDesiredNode("node-hot-2", DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
                desiredNodes = DesiredNodes.create(
                    "history",
                    1,
                    List.of(
                        actualizedDesiredNode(HOT_DESIRED_NODE),
                        pendingDesiredNode(newHotNode),
                        actualizedDesiredNode(WARM_DESIRED_NODE)
                    )
                );
            }

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold,data_warm,data_hot"),
                    nodes,
                    desiredNodes
                ),
                equalTo(Optional.of("data_cold"))
            );

            // Once all the nodes have joined, we can move the shard to the next tier
            final var updatedDesiredNodes = DesiredNodes.create(
                "history",
                2,
                desiredNodes.nodes().stream().map(DesiredNodeWithStatus::desiredNode).map(this::actualizedDesiredNode).toList()
            );

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold,data_warm,data_hot"),
                    nodes,
                    updatedDesiredNodes
                ),
                equalTo(Optional.of("data_warm"))
            );
        }
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

    private ClusterState clusterStateWithIndexAndNodes(String tierPreference, DiscoveryNodes discoveryNodes, DesiredNodes desiredNodes) {
        final Metadata.Builder metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shard.getIndexName())
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                            .put(IndexMetadata.SETTING_INDEX_UUID, shard.getIndexName())
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(DataTier.TIER_PREFERENCE, tierPreference)
                            .build()
                    )
            );
        if (desiredNodes != null) {
            metadata.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes));
        }
        return ClusterState.builder(new ClusterName("test")).nodes(discoveryNodes).metadata(metadata).build();
    }

    private static DesiredNode newDesiredNode(String externalId, DiscoveryNodeRole... roles) {
        assert roles.length > 0;

        return new DesiredNode(
            Settings.builder()
                .put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId)
                .put(NODE_ROLES_SETTING.getKey(), Arrays.stream(roles).map(DiscoveryNodeRole::roleName).collect(Collectors.joining(",")))
                .build(),
            1,
            ByteSizeValue.ONE,
            ByteSizeValue.ONE,
            Version.CURRENT
        );
    }

    private DesiredNodes createDesiredNodesWithActualizedNodes(DesiredNode... nodes) {
        return createDesiredNodesWithStatus(DesiredNodeWithStatus.Status.ACTUALIZED, nodes);
    }

    private DesiredNodes createDesiredNodesWithPendingNodes(DesiredNode... nodes) {
        return createDesiredNodesWithStatus(DesiredNodeWithStatus.Status.PENDING, nodes);
    }

    private DesiredNodes createDesiredNodesWithStatus(DesiredNodeWithStatus.Status status, DesiredNode... nodes) {
        return DesiredNodes.create(
            randomAlphaOfLength(10),
            1,
            Arrays.stream(nodes).map(desiredNode -> new DesiredNodeWithStatus(desiredNode, status)).toList()
        );
    }

    private void assertAllocationDecision(ClusterState state, DiscoveryNode node, Decision.Type decisionType, String explanationMessage) {
        final var allocation = new RoutingAllocation(allocationDeciders, null, state, null, null, 0);
        allocation.debugDecision(true);

        final var routingNode = RoutingNodesHelper.routingNode(node.getId(), node, shard);
        {
            final var decision = DataTierAllocationDecider.INSTANCE.canAllocate(shard, routingNode, allocation);
            assertThat(routingNode.toString(), decision.type(), equalTo(decisionType));
            assertThat(routingNode.toString(), decision.getExplanation(), containsString(explanationMessage));
        }

        {
            final var decision = DataTierAllocationDecider.INSTANCE.canRemain(
                allocation.metadata().getIndexSafe(shard.index()),
                shard,
                routingNode,
                allocation
            );
            assertThat(routingNode.toString(), decision.type(), equalTo(decisionType));
            assertThat(routingNode.toString(), decision.getExplanation(), containsString(explanationMessage));
        }
    }

    private DesiredNodeWithStatus actualizedDesiredNode(DesiredNode desiredNode) {
        return new DesiredNodeWithStatus(desiredNode, DesiredNodeWithStatus.Status.ACTUALIZED);
    }

    private DesiredNodeWithStatus pendingDesiredNode(DesiredNode desiredNode) {
        return new DesiredNodeWithStatus(desiredNode, DesiredNodeWithStatus.Status.PENDING);
    }
}
