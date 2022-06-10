/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class DesiredNodesTests extends DesiredNodesTestCase {
    public void testDuplicatedExternalIDsAreNotAllowed() {
        final String duplicatedExternalID = UUIDs.randomBase64UUID();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DesiredNodes.create(
                UUIDs.randomBase64UUID(),
                2,
                randomList(
                    2,
                    10,
                    () -> new DesiredNodeWithStatus(
                        randomDesiredNode(Settings.builder().put(NODE_EXTERNAL_ID_SETTING.getKey(), duplicatedExternalID).build()),
                        randomFrom(DesiredNodeWithStatus.Status.values())
                    )
                )
            )
        );
        assertThat(exception.getMessage(), containsString("Some nodes contain the same setting value"));
    }

    public void testPreviousVersionsWithSameHistoryIDAreSuperseded() {
        final DesiredNodes desiredNodes = DesiredNodes.create(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes previousDesiredNodes = DesiredNodes.create(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(previousDesiredNodes), is(equalTo(false)));
    }

    public void testIsSupersededByADifferentHistoryID() {
        final DesiredNodes desiredNodes = DesiredNodes.create(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes differentHistoryID = DesiredNodes.create(
            UUIDs.randomBase64UUID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        assertThat(desiredNodes.isSupersededBy(differentHistoryID), is(equalTo(true)));
    }

    public void testHasSameVersion() {
        final DesiredNodes desiredNodes = DesiredNodes.create(UUIDs.randomBase64UUID(), 2, Collections.emptyList());

        final DesiredNodes desiredNodesWithDifferentVersion = DesiredNodes.create(
            desiredNodes.historyID(),
            desiredNodes.version() - 1,
            Collections.emptyList()
        );

        final DesiredNodes desiredNodesWithDifferentHistoryID = DesiredNodes.create(
            UUIDs.randomBase64UUID(),
            desiredNodes.version(),
            Collections.emptyList()
        );

        assertThat(desiredNodes.hasSameVersion(desiredNodes), is(equalTo(true)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentVersion), is(equalTo(false)));
        assertThat(desiredNodes.hasSameVersion(desiredNodesWithDifferentHistoryID), is(equalTo(false)));
    }

    public void testClusterStateRemainsTheSameIfThereAreNoDesiredNodesDuringMembershipInfoUpgrade() {
        final var clusterState = ClusterState.builder(new ClusterName("test")).build();

        assertThat(DesiredNodes.updateDesiredNodesStatusIfNeeded(clusterState), is(sameInstance(clusterState)));
    }

    public void testDesiredNodesStatusIsUpdatedUsingCurrentClusterNodes() {
        final var actualizedDesiredNodes = randomList(0, 5, this::createActualizedDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);
        final var joiningDesiredNodes = randomList(1, 5, this::createPendingDesiredNode);

        final var discoveryNodes = DiscoveryNodes.builder();

        for (DesiredNodeWithStatus actualizedDesiredNode : actualizedDesiredNodes) {
            assertThat(actualizedDesiredNode.actualized(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(actualizedDesiredNode.externalId()));
        }

        for (DesiredNodeWithStatus joiningDesiredNode : joiningDesiredNodes) {
            assertThat(joiningDesiredNode.pending(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(joiningDesiredNode.externalId()));
        }

        // Add some nodes in the cluster that are not part of the desired nodes
        for (int i = 0; i < randomInt(5); i++) {
            discoveryNodes.add(newDiscoveryNode(UUIDs.randomBase64UUID(random())));
        }

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes, joiningDesiredNodes);

        final var clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes)).build())
            .build();

        final var updatedClusterState = DesiredNodes.updateDesiredNodesStatusIfNeeded(clusterState);
        assertThat(updatedClusterState, is(not(sameInstance(clusterState))));

        final var expectedActualizedNodes = Stream.concat(actualizedDesiredNodes.stream(), joiningDesiredNodes.stream())
            .map(DesiredNodeWithStatus::desiredNode)
            .toList();
        final var expectedPendingNodes = pendingDesiredNodes.stream().map(DesiredNodeWithStatus::desiredNode).toList();
        assertDesiredNodesStatusIsCorrect(updatedClusterState, expectedActualizedNodes, expectedPendingNodes);
    }

    public void testClusterStateIsNotChangedWhenDesiredNodesStatusDoNotChange() {
        final var actualizedDesiredNodes = randomList(1, 5, this::createActualizedDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var discoveryNodes = DiscoveryNodes.builder();
        for (DesiredNodeWithStatus actualizedDesiredNode : actualizedDesiredNodes) {
            assertThat(actualizedDesiredNode.actualized(), is(equalTo(true)));

            discoveryNodes.add(newDiscoveryNode(actualizedDesiredNode.externalId()));
        }

        // Add some nodes in the cluster that are not part of the desired nodes
        for (int i = 0; i < randomInt(5); i++) {
            discoveryNodes.add(newDiscoveryNode(UUIDs.randomBase64UUID(random())));
        }

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        final var clusterState = ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes)).build())
            .build();

        final var updatedClusterState = DesiredNodes.updateDesiredNodesStatusIfNeeded(clusterState);
        assertThat(updatedClusterState, is(sameInstance(clusterState)));
    }

    public void testNewDesiredNodesAreStoredInClusterStateIfTheyChange() {
        final var metadata = Metadata.builder();

        DesiredNodes previousDesiredNodes = null;
        final boolean desiredNodesInClusterState = randomBoolean();
        if (desiredNodesInClusterState) {
            final var actualizedDesiredNodes = randomList(1, 5, this::createActualizedDesiredNode);
            final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

            previousDesiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

            metadata.putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(previousDesiredNodes));
        }

        final var clusterState = ClusterState.builder(new ClusterName("test")).metadata(metadata.build()).build();

        final var newDesiredNodes = createDesiredNodes(
            // Reuse the same nodes from the previous desired nodes or create new ones
            desiredNodesInClusterState && randomBoolean()
                ? previousDesiredNodes.nodes()
                    .stream()
                    .map(dn -> new DesiredNodeWithStatus(dn.desiredNode(), DesiredNodeWithStatus.Status.PENDING))
                    .toList()
                : randomList(1, 10, this::createPendingDesiredNode)
        );

        final var updatedClusterState = DesiredNodes.updateDesiredNodesStatusIfNeeded(clusterState, newDesiredNodes);

        assertThat(updatedClusterState, is(not(sameInstance(clusterState))));
        assertThat(DesiredNodes.latestFromClusterState(updatedClusterState), is(equalTo(newDesiredNodes)));
        for (DesiredNodeWithStatus desiredNode : DesiredNodes.latestFromClusterState(updatedClusterState)) {
            assertThat(desiredNode.actualized(), is(equalTo(false)));
        }
    }

    public void testNodesStatusIsCarriedOverInNewVersions() {
        final var actualizedDesiredNodes = randomList(1, 5, this::createActualizedDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var desiredNodes = createDesiredNodes(actualizedDesiredNodes, pendingDesiredNodes);

        final var actualizedKnownDesiredNodes = randomSubsetOf(desiredNodes.actualized()).stream()
            .map(desiredNode -> randomBoolean() ? desiredNode : desiredNodeWithDifferentSpecsAndSameExternalId(desiredNode))
            .toList();
        final var newPendingDesiredNodes = randomList(actualizedKnownDesiredNodes.isEmpty() ? 1 : 0, 5, this::createPendingDesiredNode);

        final var expectedPendingNodes = Stream.concat(pendingDesiredNodes.stream(), newPendingDesiredNodes.stream())
            .map(DesiredNodeWithStatus::desiredNode)
            .toList();
        final boolean withNewHistoryId = randomBoolean();
        final var historyId = withNewHistoryId ? UUIDs.randomBase64UUID(random()) : desiredNodes.historyID();
        final var newDesiredNodesWithMembershipInformation = DesiredNodes.createIncludingStatusFromPreviousVersion(
            historyId,
            desiredNodes.version() + 1,
            concatLists(actualizedKnownDesiredNodes, expectedPendingNodes),
            desiredNodes
        );

        if (withNewHistoryId) {
            for (DesiredNodeWithStatus desiredNode : newDesiredNodesWithMembershipInformation) {
                assertThat(desiredNode.pending(), is(equalTo(true)));
            }
        } else {
            assertDesiredNodesStatusIsCorrect(newDesiredNodesWithMembershipInformation, actualizedKnownDesiredNodes, expectedPendingNodes);
        }
    }

    @SafeVarargs
    private static DesiredNodes createDesiredNodes(List<DesiredNodeWithStatus>... nodeLists) {
        assertThat(nodeLists.length, is(greaterThan(0)));
        final List<DesiredNodeWithStatus> desiredNodes = new ArrayList<>();
        for (List<DesiredNodeWithStatus> nodeList : nodeLists) {
            desiredNodes.addAll(nodeList);
        }
        return DesiredNodes.create(randomAlphaOfLength(10), randomInt(10), desiredNodes);
    }

    private DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
    }

    private DesiredNodeWithStatus createActualizedDesiredNode() {
        return new DesiredNodeWithStatus(randomDesiredNode(), DesiredNodeWithStatus.Status.ACTUALIZED);
    }

    private DesiredNodeWithStatus createPendingDesiredNode() {
        return new DesiredNodeWithStatus(randomDesiredNode(), DesiredNodeWithStatus.Status.PENDING);
    }

    private DesiredNode desiredNodeWithDifferentSpecsAndSameExternalId(DesiredNode desiredNode) {
        return new DesiredNode(
            desiredNode.settings(),
            desiredNode.minProcessors() + randomIntBetween(1, 10),
            ByteSizeValue.ofGb(desiredNode.memory().getGb() + randomIntBetween(15, 20)),
            ByteSizeValue.ofGb(desiredNode.storage().getGb() + randomIntBetween(1, 100)),
            Version.CURRENT
        );
    }
}
