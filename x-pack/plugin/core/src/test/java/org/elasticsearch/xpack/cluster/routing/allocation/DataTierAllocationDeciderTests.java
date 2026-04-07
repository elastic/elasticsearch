/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import joptsimple.internal.Strings;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.xpack.core.XPackPlugin;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_COLD;
import static org.elasticsearch.cluster.routing.allocation.DataTier.DATA_FROZEN;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.NodeRoleSettings.NODE_ROLES_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class DataTierAllocationDeciderTests extends ESAllocationTestCase {

    private static final DiscoveryNode HOT_NODE = newNode("node-hot", Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
    private static final DiscoveryNode WARM_NODE = newNode("node-warm", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
    private static final DiscoveryNode WARM_NODE_TWO = newNode("node-warm-2", Collections.singleton(DiscoveryNodeRole.DATA_WARM_NODE_ROLE));
    private static final DiscoveryNode COLD_NODE = newNode("node-cold", Collections.singleton(DiscoveryNodeRole.DATA_COLD_NODE_ROLE));
    private static final DiscoveryNode CONTENT_NODE = newNode(
        "node-content",
        Collections.singleton(DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE)
    );
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private static final DiscoveryNode DATA_NODE_TWO = newNode("node-data-2", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));

    private static final DesiredNode HOT_DESIRED_NODE = newDesiredNode("node-hot", DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
    private static final DesiredNode WARM_DESIRED_NODE = newDesiredNode("node-warm", DiscoveryNodeRole.DATA_WARM_NODE_ROLE);
    private static final DesiredNode COLD_DESIRED_NODE = newDesiredNode("node-cold", DiscoveryNodeRole.DATA_COLD_NODE_ROLE);
    private static final DesiredNode CONTENT_DESIRED_NODE = newDesiredNode("node-content", DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE);
    private static final DesiredNode DATA_DESIRED_NODE = newDesiredNode("node-data", DiscoveryNodeRole.DATA_ROLE);

    private final ClusterSettings clusterSettings = createBuiltInClusterSettings();
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            DataTierAllocationDecider.INSTANCE,
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        )
    );

    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
        ShardRouting.Role.DEFAULT
    );

    public void testIndexPrefer() {
        {
            final var desiredNodes = randomBoolean() ? null : createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE);
            final ProjectId projectId = randomProjectIdOrDefault();
            final var clusterState = clusterStateWithIndexAndNodes(
                projectId,
                "data_warm,data_cold",
                DiscoveryNodes.builder().add(HOT_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    projectId,
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold], "
                        + "but no nodes for any of those tiers are available in the cluster"
                );
            }
        }

        {
            final var desiredNodes = randomBoolean() ? null : createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, COLD_DESIRED_NODE);
            final ProjectId projectId = randomProjectIdOrDefault();
            final var clusterState = clusterStateWithIndexAndNodes(
                projectId,
                "data_warm,data_cold",
                DiscoveryNodes.builder().add(HOT_NODE).add(COLD_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode n : Arrays.asList(HOT_NODE, WARM_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    projectId,
                    n,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_warm,data_cold] and node does not meet the required [data_cold] tier"
                );
            }

            assertAllocationDecision(
                clusterState,
                projectId,
                COLD_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_warm,data_cold] and node has tier [data_cold]"
            );
        }

        {
            // Remove the cold tier from desired nodes
            final var desiredNodes = createDesiredNodesWithActualizedNodes(WARM_DESIRED_NODE);
            final ProjectId projectId = randomProjectIdOrDefault();
            final var state = clusterStateWithIndexAndNodes(
                projectId,
                "data_cold,data_warm",
                DiscoveryNodes.builder().add(WARM_NODE).add(COLD_NODE).build(),
                desiredNodes
            );

            for (DiscoveryNode node : List.of(HOT_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    state,
                    projectId,
                    node,
                    Decision.Type.NO,
                    "index has a preference for tiers [data_cold,data_warm] and node does not meet the required [data_warm] tier"
                );
            }

            assertAllocationDecision(
                state,
                projectId,
                WARM_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_cold,data_warm] and node has tier [data_warm]"
            );
        }

        {
            // There's a warm node in the desired nodes, but it hasn't joined the cluster yet,
            // in that case we consider that there aren't any nodes with the preferred tier in the cluster
            final ProjectId projectId = randomProjectIdOrDefault();
            final ClusterState clusterState;
            final String tierPreference;
            if (randomBoolean()) {
                tierPreference = "data_warm,data_cold";
                clusterState = clusterStateWithIndexAndNodes(
                    projectId,
                    tierPreference,
                    DiscoveryNodes.builder().add(HOT_NODE).build(),
                    DesiredNodes.create("history", 1, List.of(pendingDesiredNode(WARM_DESIRED_NODE)))
                );
            } else {
                tierPreference = "data_warm,data_hot";
                clusterState = clusterStateWithIndexAndNodes(
                    projectId,
                    tierPreference,
                    DiscoveryNodes.builder().add(COLD_NODE).build(),
                    DesiredNodes.create("history", 1, List.of(pendingDesiredNode(WARM_DESIRED_NODE)))
                );
            }

            for (DiscoveryNode node : List.of(HOT_NODE, WARM_NODE, COLD_NODE)) {
                assertAllocationDecision(
                    clusterState,
                    projectId,
                    node,
                    Decision.Type.NO,
                    org.elasticsearch.core.Strings.format(
                        "index has a preference for tiers [%s], but no nodes for any of those tiers are available in the cluster",
                        tierPreference
                    )
                );
            }
        }
        {
            final ProjectId projectId = randomProjectIdOrDefault();
            final var state = clusterStateWithIndexAndNodes(projectId, "data_warm", DiscoveryNodes.builder().add(DATA_NODE).build(), null);

            assertAllocationDecision(
                state,
                projectId,
                DATA_NODE,
                Decision.Type.YES,
                "index has a preference for tiers [data_warm] and node has tier [data]"
            );
        }
    }

    public void testTierNodesPresent() {
        DiscoveryNodes nodes = DiscoveryNodes.builder().build();

        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_cold", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_content", nodes, irrelevantNodeIds(nodes)));

        nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();

        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", nodes, irrelevantNodeIds(nodes)));
        assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_cold", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_content", nodes, irrelevantNodeIds(nodes)));

        nodes = DiscoveryNodes.builder().add(DATA_NODE).build();

        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_cold", nodes, irrelevantNodeIds(nodes)));
        assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_content", nodes, irrelevantNodeIds(nodes)));
    }

    public void testTierNodesPresentWithRelevantNodeShutdowns() {
        {
            DiscoveryNodes nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(DATA_NODE).build();

            assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", nodes, Set.of(HOT_NODE.getId())));
            assertFalse(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_hot",
                    nodes,
                    Set.of(HOT_NODE.getId(), DATA_NODE.getId())
                )
            );

            assertTrue(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_warm",
                    nodes,
                    Set.of(HOT_NODE.getId(), DATA_NODE.getId())
                )
            );
            assertFalse(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_warm",
                    nodes,
                    Set.of(HOT_NODE.getId(), WARM_NODE.getId(), DATA_NODE.getId())
                )
            );

            assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_cold", nodes, Set.of(HOT_NODE.getId())));
            assertFalse(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_cold",
                    nodes,
                    Set.of(HOT_NODE.getId(), DATA_NODE.getId())
                )
            );
        }

        {
            DiscoveryNodes onlyTierNodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(WARM_NODE_TWO).build();
            assertFalse(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", onlyTierNodes, Set.of(HOT_NODE.getId())));
            assertTrue(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", onlyTierNodes, Set.of(WARM_NODE.getId()))
            );
            assertFalse(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_warm",
                    onlyTierNodes,
                    Set.of(WARM_NODE.getId(), WARM_NODE_TWO.getId())
                )
            );
        }

        {
            DiscoveryNodes nodes = DiscoveryNodes.builder().add(HOT_NODE).add(DATA_NODE).add(DATA_NODE_TWO).build();
            assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_hot", nodes, Set.of(HOT_NODE.getId())));
            assertTrue(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_hot",
                    nodes,
                    Set.of(HOT_NODE.getId(), DATA_NODE.getId())
                )
            );
            assertTrue(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_hot",
                    nodes,
                    Set.of(HOT_NODE.getId(), DATA_NODE_TWO.getId())
                )
            );
            assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", nodes, Set.of(DATA_NODE.getId())));
            assertTrue(DataTierAllocationDecider.tierNodesPresentConsideringRemovals("data_warm", nodes, Set.of(DATA_NODE_TWO.getId())));
            assertFalse(
                DataTierAllocationDecider.tierNodesPresentConsideringRemovals(
                    "data_warm",
                    nodes,
                    Set.of(DATA_NODE.getId(), DATA_NODE_TWO.getId())
                )
            );

        }
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
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_warm"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_warm,data_content"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.empty())
            );
        }

        {
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            final var desiredNodes = randomBoolean()
                ? null
                : createDesiredNodesWithActualizedNodes(WARM_DESIRED_NODE, CONTENT_DESIRED_NODE);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_warm"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_warm,data_content"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes,
                    desiredNodes == null
                        ? irrelevantNodesShutdownMetadata(nodes)
                        : nodesShutdownMetadataForDesiredNodesTests(desiredNodes, nodes)
                ),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            final var nodes = DiscoveryNodes.builder().add(WARM_NODE).add(CONTENT_NODE).build();
            final var desiredNodes = createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, WARM_DESIRED_NODE, CONTENT_DESIRED_NODE);
            final var shutdownMetadata = irrelevantNodesShutdownMetadata(nodes);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes, shutdownMetadata),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_hot"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_warm,data_content"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_hot"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
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
            final var shutdownMetadata = irrelevantNodesShutdownMetadata(nodes);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(DataTier.parseTierList("data"), nodes, desiredNodes, shutdownMetadata),
                equalTo(Optional.empty())
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_warm,data_content"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_warm"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_content,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_content"))
            );
            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_hot,data_cold,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_warm"))
            );
        }

        {
            // Cold tier is planned to be removed
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final var desiredNodes = createDesiredNodesWithActualizedNodes(HOT_DESIRED_NODE, WARM_DESIRED_NODE);
            final var shutdownMetadata = irrelevantNodesShutdownMetadata(nodes);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
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
            final var shutdownMetadata = irrelevantNodesShutdownMetadata(nodes);

            assertThat(
                DataTierAllocationDecider.preferredAvailableTier(
                    DataTier.parseTierList("data_cold,data_warm"),
                    nodes,
                    desiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_cold"))
            );
        }

        {
            // Ensure that when we are removing a tier and growing the next preferred tier we wait until all the new
            // nodes have joined the cluster avoiding filling the new nodes with shards from the removed tier
            final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
            final var shutdownMetadata = irrelevantNodesShutdownMetadata(nodes);
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
                    desiredNodes,
                    shutdownMetadata
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
                    updatedDesiredNodes,
                    shutdownMetadata
                ),
                equalTo(Optional.of("data_warm"))
            );
        }
    }

    public void testDataTierDeciderConsidersNodeShutdown() {
        final var nodes = DiscoveryNodes.builder().add(HOT_NODE).add(WARM_NODE).add(COLD_NODE).build();
        final DesiredNodes desiredNodes = null; // Desired nodes will take precedence over node shutdown if it is present

        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(
                DataTier.parseTierList("data_warm,data_cold,data_hot"),
                nodes,
                desiredNodes,
                new NodesShutdownMetadata(Map.of(WARM_NODE.getId(), randomShutdownMetadataRemovingNode(WARM_NODE.getId())))
            ),
            equalTo(Optional.of("data_cold"))
        );

        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(
                DataTier.parseTierList("data_warm,data_cold,data_hot"),
                nodes,
                desiredNodes,
                new NodesShutdownMetadata(
                    Map.of(
                        WARM_NODE.getId(),
                        randomShutdownMetadataRemovingNode(WARM_NODE.getId()),
                        COLD_NODE.getId(),
                        randomShutdownMetadataRemovingNode(COLD_NODE.getId())
                    )
                )
            ),
            equalTo(Optional.of("data_hot"))
        );

        assertThat(
            DataTierAllocationDecider.preferredAvailableTier(
                DataTier.parseTierList("data_warm,data_cold,data_hot"),
                nodes,
                desiredNodes,
                new NodesShutdownMetadata(
                    Map.of(
                        WARM_NODE.getId(),
                        randomShutdownMetadataRemovingNode(WARM_NODE.getId()),
                        COLD_NODE.getId(),
                        randomShutdownMetadataRemovingNode(COLD_NODE.getId()),
                        HOT_NODE.getId(),
                        randomShutdownMetadataRemovingNode(HOT_NODE.getId())
                    )
                )
            ),
            equalTo(Optional.empty())
        );

    }

    private SingleNodeShutdownMetadata randomShutdownMetadataRemovingNode(String nodeId) {
        SingleNodeShutdownMetadata.Type type = randomFrom(
            SingleNodeShutdownMetadata.Type.SIGTERM,
            SingleNodeShutdownMetadata.Type.REPLACE,
            SingleNodeShutdownMetadata.Type.REMOVE
        );
        var builder = SingleNodeShutdownMetadata.builder()
            .setNodeId(nodeId)
            .setNodeEphemeralId(nodeId)
            .setType(type)
            .setReason(this.getTestName());
        return switch (type) {
            case REMOVE -> builder.setStartedAtMillis(randomNonNegativeLong()).build();
            case REPLACE -> builder.setTargetNodeName(randomAlphaOfLength(10)).setStartedAtMillis(randomNonNegativeLong()).build();
            case SIGTERM -> builder.setGracePeriod(randomTimeValue()).setStartedAtMillis(randomNonNegativeLong()).build();
            case RESTART -> throw new AssertionError("bad randomization, this method only generates removal type shutdowns");
        };
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

    private ClusterState clusterStateWithIndexAndNodes(
        ProjectId projectId,
        String tierPreference,
        DiscoveryNodes discoveryNodes,
        DesiredNodes desiredNodes
    ) {
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(shard.getIndexName())
                    .settings(
                        indexSettings(IndexVersion.current(), 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, shard.getIndexName())
                            .put(DataTier.TIER_PREFERENCE, tierPreference)
                    )
                    .build(),
                true
            )
            .build();
        Metadata.Builder metadataBuilder = Metadata.builder().put(projectMetadata);
        if (desiredNodes != null) {
            metadataBuilder.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes));
        }

        RoutingTable.Builder routingTableBuilder = new RoutingTable.Builder();
        routingTableBuilder.add(IndexRoutingTable.builder(shard.shardId().getIndex()).build());

        return ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(metadataBuilder)
            .routingTable(GlobalRoutingTable.builder().put(projectId, routingTableBuilder.build()).build())
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
            ByteSizeValue.ONE
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

    private void assertAllocationDecision(
        ClusterState state,
        ProjectId projectId,
        DiscoveryNode node,
        Decision.Type decisionType,
        String explanationMessage
    ) {
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
                allocation.metadata().getProject(projectId).getIndexSafe(shard.index()),
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

    /**
     * Creates node shutdown metadata that should not impact the decider, either because it is empty or because it is irrelevant to the
     * decider logic.
     */
    private NodesShutdownMetadata irrelevantNodesShutdownMetadata(DiscoveryNodes currentNodes) {
        final Set<String> currentNodeIds = currentNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet());
        int kind = currentNodes.size() == 0 ? randomFrom(1, 2) : randomFrom(1, 2, 3);
        return switch (kind) {
            case 1 -> new NodesShutdownMetadata(Collections.emptyMap());
            case 2 -> randomRemovalNotInCluster(currentNodeIds);
            case 3 -> randomRestartInCluster(currentNodeIds);
            default -> throw new AssertionError("not all randomization branches covered in test");
        };
    }

    /**
     * Desired Nodes should take precedence over node shutdown if it's in use, so this method generates node shutdown metadata that
     * intersects with current or desired nodes. The output of this function shouldn't matter, whatever node shutdowns it generates; if it
     * does there's a bug.
     */
    private NodesShutdownMetadata nodesShutdownMetadataForDesiredNodesTests(DesiredNodes desiredNodes, DiscoveryNodes currentNodes) {
        // Note that the desired node's External ID is not the same as the final node ID, but mix them in anyway
        Set<String> nodeIds = Stream.concat(
            desiredNodes != null ? desiredNodes.nodes().stream().map(DesiredNodeWithStatus::externalId) : Stream.empty(),
            currentNodes.stream().map(DiscoveryNode::getId)
        ).collect(Collectors.toSet());
        if (nodeIds.isEmpty()) {
            return new NodesShutdownMetadata(Collections.emptyMap());
        }
        int kind = randomFrom(1, 2);
        return switch (kind) {
            case 1 -> new NodesShutdownMetadata(Collections.emptyMap());
            case 2 -> randomRemovalInCluster(nodeIds);
            default -> throw new AssertionError("not all randomization branches covered in test");
        };
    }

    private Set<String> irrelevantNodeIds(DiscoveryNodes currentNodes) {
        Set<String> nodeIds = new HashSet<>();
        int numIds = randomIntBetween(0, 10);
        for (int i = 0; i < numIds; i++) {
            nodeIds.add(
                randomValueOtherThanMany((val) -> currentNodes.nodeExists(val) || nodeIds.contains(val), () -> randomAlphaOfLength(10))
            );
        }
        return nodeIds;
    }

    private NodesShutdownMetadata randomRemovalNotInCluster(Set<String> currentNodes) {
        String nodeId = randomValueOtherThanMany(currentNodes::contains, () -> randomAlphaOfLength(10));
        return new NodesShutdownMetadata(Map.of(nodeId, randomShutdownMetadataRemovingNode(nodeId)));
    }

    private NodesShutdownMetadata randomRemovalInCluster(Set<String> currentNodes) {
        String nodeId = randomFrom(currentNodes);
        return new NodesShutdownMetadata(Map.of(nodeId, randomShutdownMetadataRemovingNode(nodeId)));
    }

    private NodesShutdownMetadata randomRestartInCluster(Set<String> currentNodes) {
        String nodeId = randomFrom(currentNodes);
        return new NodesShutdownMetadata(
            Map.of(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setNodeEphemeralId(nodeId)
                    .setType(SingleNodeShutdownMetadata.Type.RESTART)
                    .setReason(this.getTestName())
                    .setStartedAtMillis(randomNonNegativeLong())
                    .build()
            )
        );
    }

    public void testClusterConcurrentRebalanceIndependentLimits() {
        final Set<DiscoveryNodeRole> hotRole = Collections.singleton(DiscoveryNodeRole.DATA_HOT_NODE_ROLE);
        final Set<DiscoveryNodeRole> frozenRole = Collections.singleton(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE);

        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 3)
            .put("cluster.routing.allocation.cluster_concurrent_frozen_rebalance", 7)
            .build();

        AllocationService strategy = createAllocationService(
            settings,
            new TestGatewayAllocator(),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES,
            Arrays.asList(new XPackPlugin(settings))
        );

        logger.info("Building initial routing table");
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder("test")
                    .settings(settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT))
                    .numberOfShards(5)
                    .numberOfReplicas(1)
                    .build(),
                true
            )
            .put(
                IndexMetadata.builder("test_frozen")
                    .settings(
                        settings(IndexVersion.current()).put(DataTier.TIER_PREFERENCE, DataTier.DATA_FROZEN)
                            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE)
                            .put(SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_PARTIAL_SETTING_KEY, true)
                    )
                    .numberOfShards(5)
                    .numberOfReplicas(1)
                    .build(),
                true
            )
            .build();
        Metadata metadata = Metadata.builder().put(projectMetadata).build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(projectMetadata.index("test"))
            .addAsNew(projectMetadata.index("test_frozen"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .putRoutingTable(projectId, initialRoutingTable)
            .build();

        IndexRoutingTable index = clusterState.routingTable(projectId).index("test");
        assertThat(index.size(), equalTo(5));
        assertShardsUnassigned(index);

        IndexRoutingTable frozenIndex = clusterState.routingTable(projectId).index("test_frozen");
        assertThat(frozenIndex.size(), equalTo(5));
        assertShardsUnassigned(index);

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node1", hotRole))
                    .add(newNode("node2", hotRole))
                    .add(newNode("node1_frozen", frozenRole))
                    .add(newNode("node2_frozen", frozenRole))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        index = clusterState.routingTable(projectId).index("test");
        assertPrimariesInitializing(index);

        frozenIndex = clusterState.routingTable(projectId).index("test_frozen");
        assertPrimariesInitializing(frozenIndex);

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        index = clusterState.routingTable(projectId).index("test");
        assertReplicasInitializing(index);

        frozenIndex = clusterState.routingTable(projectId).index("test_frozen");
        assertReplicasInitializing(frozenIndex);

        logger.info("now, start 8 more frozen and hot nodes");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(newNode("node3", hotRole))
                    .add(newNode("node4", hotRole))
                    .add(newNode("node5", hotRole))
                    .add(newNode("node6", hotRole))
                    .add(newNode("node7", hotRole))
                    .add(newNode("node8", hotRole))
                    .add(newNode("node9", hotRole))
                    .add(newNode("node10", hotRole))
                    .add(newNode("node3_frozen", frozenRole))
                    .add(newNode("node4_frozen", frozenRole))
                    .add(newNode("node5_frozen", frozenRole))
                    .add(newNode("node6_frozen", frozenRole))
                    .add(newNode("node7_frozen", frozenRole))
                    .add(newNode("node8_frozen", frozenRole))
                    .add(newNode("node9_frozen", frozenRole))
                    .add(newNode("node10_frozen", frozenRole))
            )
            .build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Hot should be able to relocate its max of 3 shards, and frozen its max of 7 shards");

        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertThat(shardsWithState(routingNodes, "test", STARTED).size(), equalTo(7));
        assertThat(shardsWithState(routingNodes, "test", RELOCATING).size(), equalTo(3));

        assertThat(shardsWithState(routingNodes, "test_frozen", STARTED).size(), equalTo(3));
        assertThat(shardsWithState(routingNodes, "test_frozen", RELOCATING).size(), equalTo(7));
    }

    void assertShardsUnassigned(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, UNASSIGNED, UNASSIGNED);

        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRouting = indexRoutingTable.shard(i);
            assertThat(shardRouting.shard(0).currentNodeId(), nullValue());
            assertThat(shardRouting.shard(1).currentNodeId(), nullValue());
        }
    }

    void assertPrimariesInitializing(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, INITIALIZING, UNASSIGNED);
    }

    void assertReplicasInitializing(IndexRoutingTable indexRoutingTable) {
        assertShardStates(indexRoutingTable, STARTED, INITIALIZING);
    }

    void assertShardStates(IndexRoutingTable indexRoutingTable, ShardRoutingState primaryState, ShardRoutingState replicaState) {
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRouting = indexRoutingTable.shard(i);
            assertThat(shardRouting.size(), equalTo(2));
            assertThat(shardRouting.primaryShard().state(), equalTo(primaryState));
            assertThat(shardRouting.replicaShards().get(0).state(), equalTo(replicaState));
        }
    }
}
