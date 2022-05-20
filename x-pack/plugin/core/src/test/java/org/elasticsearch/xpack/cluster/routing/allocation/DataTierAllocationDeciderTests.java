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
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.FakeDesiredNodesMembershipService;
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
import org.elasticsearch.common.unit.ByteSizeValue;
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
    private final AllocationService service = new AllocationService(
        allocationDeciders,
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE,
        FakeDesiredNodesMembershipService.INSTANCE
    );

    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created")
    );

    public void testIndexPrefer() {
        {
            final var clusterState = clusterStateWithIndexAndNodes("data_warm,data_cold", DiscoveryNodes.builder().add(HOT_NODE).build());

            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    randomBoolean()
                        ? DesiredNodes.MembershipInformation.EMPTY
                        : createDesiredNodesMembershipInformation(Set.of(HOT_DESIRED_NODE)),
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                );
            }
        }

        {
            final var clusterState = clusterStateWithIndexAndNodes(
                "data_warm,data_cold",
                DiscoveryNodes.builder().add(HOT_NODE).add(COLD_NODE).build()
            );

            final var desiredNodesMembers = randomBoolean()
                ? DesiredNodes.MembershipInformation.EMPTY
                : createDesiredNodesMembershipInformation(Set.of(HOT_DESIRED_NODE, COLD_DESIRED_NODE));
            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    desiredNodesMembers,
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold] and node does not meet the required [data_cold] tier"
                );
            }

            assertAllocationDecision(
                clusterState,
                desiredNodesMembers,
                COLD_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]"
            );
        }

        {
            final var state = clusterStateWithIndexAndNodes(
                "data_cold,data_warm",
                DiscoveryNodes.builder().add(WARM_NODE).add(COLD_NODE).build()
            );

            for (DiscoveryNode node : List.of(HOT_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    state,
                    createDesiredNodesMembershipInformation(Set.of(WARM_DESIRED_NODE)),
                    node,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_cold,data_warm] and node does not meet the required [data_warm] tier"
                );
            }

            assertAllocationDecision(
                state,
                createDesiredNodesMembershipInformation(Set.of(WARM_DESIRED_NODE)),
                WARM_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_cold,data_warm] and node has tier [data_warm]"
            );
        }

        {
            // There's a warm node in the desired nodes, but it hasn't joined the cluster yet,
            // in that case we consider that there aren't any nodes with the preferred tier in the cluster
            final var clusterState = clusterStateWithIndexAndNodes("data_warm,data_cold", DiscoveryNodes.builder().add(HOT_NODE).build());

            for (DiscoveryNode node : List.of(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    createDesiredNodesMembershipInformation(Set.of(HOT_DESIRED_NODE)),
                    node,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                );
            }
        }
    }

    private DesiredNodes.MembershipInformation createDesiredNodesMembershipInformation(Set<DesiredNode> members) {
        return new DesiredNodes.MembershipInformation(new DesiredNodes(randomAlphaOfLength(10), 1, List.copyOf(members)), members);
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
            final var desiredNodes = DesiredNodes.MembershipInformation.EMPTY;

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
            final var memberDesiredNodes = randomBoolean()
                ? DesiredNodes.MembershipInformation.EMPTY
                : createDesiredNodesMembershipInformation(Set.of(WARM_DESIRED_NODE, CONTENT_DESIRED_NODE));

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, memberDesiredNodes),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_hot,data_warm"), nodes, memberDesiredNodes),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_warm,data_content"),
                    nodes,
                    memberDesiredNodes
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_content,data_warm"),
                    nodes,
                    memberDesiredNodes
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    memberDesiredNodes
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    memberDesiredNodes
                ),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            final var desiredNodes = createDesiredNodesMembershipInformation(
                Set.of(HOT_DESIRED_NODE, WARM_DESIRED_NODE, CONTENT_DESIRED_NODE)
            );

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
            final var desiredNodes = createDesiredNodesMembershipInformation(Set.of(WARM_DESIRED_NODE, CONTENT_DESIRED_NODE));

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
            final var desiredNodes = createDesiredNodesMembershipInformation(Set.of(HOT_DESIRED_NODE, WARM_DESIRED_NODE));

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data_cold,data_warm"), nodes, desiredNodes),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            // During grow and shrink (i.e. a way to replace a node) we should avoid moving the shard from a preferred tier to a less
            // preferred tier if there's a node that can hold that shard and we know that a new desired node would substitute the old one
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final var desiredNodesMembershipInfo = new DesiredNodes.MembershipInformation(
                new DesiredNodes("history", 1, List.of(HOT_DESIRED_NODE, WARM_DESIRED_NODE, COLD_DESIRED_NODE)),
                Set.of(HOT_DESIRED_NODE, WARM_DESIRED_NODE)
            );

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold,data_warm"),
                    nodes,
                    desiredNodesMembershipInfo
                ),
                equalTo(Optional.of("data_cold"))
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

    private ClusterState clusterStateWithIndexAndNodes(String tierPreference, DiscoveryNodes discoveryNodes) {
        return ClusterState.builder(service.reroute(ClusterState.EMPTY_STATE, "initial state"))
            .nodes(discoveryNodes)
            .metadata(
                Metadata.builder()
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
                    )
            )
            .build();
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

    private void assertAllocationDecision(
        ClusterState state,
        DesiredNodes.MembershipInformation desiredNodesMembershipInformation,
        DiscoveryNode node,
        Decision.Type decisionType,
        String explanationMessage
    ) {
        final var allocation = new RoutingAllocation(allocationDeciders, null, state, null, null, desiredNodesMembershipInformation, 0);
        allocation.debugDecision(true);

        final var routingNode = new RoutingNode(node.getId(), node, shard);
        {
            final var decision = DataTierAllocationDecider.INSTANCE.canAllocate(shard, routingNode, allocation);
            assertThat(routingNode.toString(), decision.type(), equalTo(decisionType));
            assertThat(routingNode.toString(), decision.getExplanation(), containsString(explanationMessage));
        }

        {
            final var decision = DataTierAllocationDecider.INSTANCE.canRemain(shard, routingNode, allocation);
            assertThat(routingNode.toString(), decision.type(), equalTo(decisionType));
            assertThat(routingNode.toString(), decision.getExplanation(), containsString(explanationMessage));
        }
    }
}
