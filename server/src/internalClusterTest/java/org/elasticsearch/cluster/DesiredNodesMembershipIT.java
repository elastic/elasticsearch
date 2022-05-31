/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesAction;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesResponse;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.assertDesiredNodesMembershipIsCorrect;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.createDesiredNodes;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNodeWithName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DesiredNodesMembershipIT extends ESIntegTestCase {
    public void testSimpleCase() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var knownDesiredNodes = nodeNames.stream().map(this::createDesiredNode).toList();
        AtomicInteger ai = new AtomicInteger();
        final var unknownDesiredNodes = randomList(0, 5, () -> createDesiredNode("unknown-" + ai.incrementAndGet()));

        final var desiredNodes = createDesiredNodes(knownDesiredNodes, unknownDesiredNodes);

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);
        }

        final var newerDesiredNodes = createDesiredNodes(desiredNodes.historyID(), desiredNodes.version() + 1, desiredNodes.nodes());
        updateDesiredNodes(newerDesiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);
        }
    }

    public void testIdempotentUpdateWithUpdatedMembership() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var knownDesiredNodes = nodeNames.stream().map(this::createDesiredNode).toList();
        final var unknownDesiredNodes = randomList(0, 5, () -> createDesiredNode(randomAlphaOfLength(10)));

        final var desiredNodes = createDesiredNodes(knownDesiredNodes, unknownDesiredNodes);

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);
        }

        updateDesiredNodes(desiredNodes);

        {
            final var clusterState = client().admin().cluster().prepareState().get().getState();
            assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);
        }
    }

    public void testMemberDesiredNodesAreKeptAsMemberEvenIfNodesLeavesTemporarily() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var knownDesiredNodes = nodeNames.stream().map(this::createDesiredNode).toList();
        final var unknownDesiredNodes = randomList(0, 5, () -> createDesiredNode(randomAlphaOfLength(10)));

        final var desiredNodes = createDesiredNodes(knownDesiredNodes, unknownDesiredNodes);

        updateDesiredNodes(desiredNodes);

        final var clusterState = client().admin().cluster().prepareState().get().getState();
        assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);

        final var leavingNodeNames = randomSubsetOf(nodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var newClusterState = client().admin().cluster().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(newClusterState);

        for (String leavingNodeName : leavingNodeNames) {
            final var desiredNode = latestDesiredNodes.find(leavingNodeName);
            assertThat(desiredNode.isMember(), is(equalTo(true)));
        }
    }

    public void testMembershipInformationIsClearedAfterHistoryIdChanges() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var clusterNodeNames = internalCluster().startNodes(numberOfNodes);

        final var knownDesiredNodes = clusterNodeNames.stream().map(this::createDesiredNode).toList();
        final var unknownDesiredNodes = randomList(0, 5, () -> createDesiredNode(randomAlphaOfLength(10)));

        final var desiredNodes = createDesiredNodes(knownDesiredNodes, unknownDesiredNodes);

        updateDesiredNodes(desiredNodes);

        final var clusterState = client().admin().cluster().prepareState().get().getState();
        assertDesiredNodesMembershipIsCorrect(clusterState, knownDesiredNodes, unknownDesiredNodes);

        // Stop some nodes, these shouldn't be members within the new desired node's history until they join back
        final var leavingNodeNames = randomSubsetOf(clusterNodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var newDesiredNodesHistory = new DesiredNodes(UUIDs.randomBase64UUID(random()), 1, desiredNodes.nodes());

        final var response = updateDesiredNodes(newDesiredNodesHistory);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(true)));

        final var updatedClusterState = client().admin().cluster().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(updatedClusterState);

        for (String clusterNodeName : clusterNodeNames) {
            final var desiredNode = latestDesiredNodes.find(clusterNodeName);
            assertThat(desiredNode.isMember(), is(equalTo(leavingNodeNames.contains(clusterNodeName) == false)));
        }
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(DesiredNodes desiredNodes) {
        final UpdateDesiredNodesRequest request = new UpdateDesiredNodesRequest(
            desiredNodes.historyID(),
            desiredNodes.version(),
            List.copyOf(desiredNodes.nodes())
        );
        return client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    private DesiredNode createDesiredNode(String nodeName) {
        return randomDesiredNodeWithName(nodeName);
    }
}
