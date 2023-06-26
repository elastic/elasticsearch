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
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
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

        final var clusterState = createClusterStateWithDiscoveryNodesAndDesiredNodes(
            actualizedDesiredNodes,
            pendingDesiredNodes,
            joiningDesiredNodes,
            true,
            true
        );

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

        final var clusterState = createClusterStateWithDiscoveryNodesAndDesiredNodes(
            actualizedDesiredNodes,
            pendingDesiredNodes,
            Collections.emptyList(),
            true,
            true
        );

        final var updatedClusterState = DesiredNodes.updateDesiredNodesStatusIfNeeded(clusterState);
        assertThat(updatedClusterState, is(sameInstance(clusterState)));
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

    private static DesiredNodes createDesiredNodes(List<DesiredNodeWithStatus> actualized, List<DesiredNodeWithStatus> pending) {
        assertThat(actualized.stream().allMatch(DesiredNodeWithStatus::actualized), is(true));
        assertThat(pending.stream().allMatch(DesiredNodeWithStatus::pending), is(true));
        final List<DesiredNodeWithStatus> desiredNodes = concatLists(actualized, pending);
        return DesiredNodes.create(randomAlphaOfLength(10), randomInt(10), desiredNodes);
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
            desiredNode.minProcessors().count() + randomIntBetween(1, 10),
            ByteSizeValue.ofGb(desiredNode.memory().getGb() + randomIntBetween(15, 20)),
            ByteSizeValue.ofGb(desiredNode.storage().getGb() + randomIntBetween(1, 100)),
            Version.CURRENT
        );
    }
}
