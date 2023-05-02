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
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class DesiredNodesStatusIT extends ESIntegTestCase {
    public void testDesiredNodesStatusIsTracked() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::randomDesiredNodeWithName).toList();
        final var pendingDesiredNodes = randomList(0, 5, DesiredNodesTestCase::randomDesiredNode);

        final var updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            1,
            concatLists(actualizedDesiredNodes, pendingDesiredNodes),
            false
        );
        updateDesiredNodes(updateDesiredNodesRequest);

        {
            final var clusterState = clusterAdmin().prepareState().get().getState();
            assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }

        final var newVersionUpdateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            updateDesiredNodesRequest.getHistoryID(),
            updateDesiredNodesRequest.getVersion() + 1,
            updateDesiredNodesRequest.getNodes(),
            false
        );
        updateDesiredNodes(newVersionUpdateDesiredNodesRequest);

        {
            final var clusterState = clusterAdmin().prepareState().get().getState();
            assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }
    }

    public void testIdempotentUpdateWithUpdatedStatus() {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::randomDesiredNodeWithName).toList();
        final var pendingDesiredNodes = randomList(0, 5, DesiredNodesTestCase::randomDesiredNode);

        final var updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            1,
            concatLists(actualizedDesiredNodes, pendingDesiredNodes),
            false
        );
        updateDesiredNodes(updateDesiredNodesRequest);

        {
            final var clusterState = clusterAdmin().prepareState().get().getState();
            DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }

        updateDesiredNodes(updateDesiredNodesRequest);

        {
            final var clusterState = clusterAdmin().prepareState().get().getState();
            DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);
        }
    }

    public void testActualizedDesiredNodesAreKeptAsActualizedEvenIfNodesLeavesTemporarily() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var nodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = nodeNames.stream().map(this::randomDesiredNodeWithName).toList();
        final var pendingDesiredNodes = randomList(0, 5, DesiredNodesTestCase::randomDesiredNode);

        final var updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            1,
            concatLists(actualizedDesiredNodes, pendingDesiredNodes),
            false
        );
        updateDesiredNodes(updateDesiredNodesRequest);

        final var clusterState = clusterAdmin().prepareState().get().getState();
        DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);

        final var leavingNodeNames = randomSubsetOf(nodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var newClusterState = clusterAdmin().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(newClusterState);

        for (String leavingNodeName : leavingNodeNames) {
            final var desiredNode = latestDesiredNodes.find(leavingNodeName);
            assertThat(desiredNode.actualized(), is(equalTo(true)));
        }
    }

    public void testStatusInformationIsClearedAfterHistoryIdChanges() throws Exception {
        final int numberOfNodes = randomIntBetween(1, 5);

        final var clusterNodeNames = internalCluster().startNodes(numberOfNodes);

        final var actualizedDesiredNodes = clusterNodeNames.stream().map(this::randomDesiredNodeWithName).toList();
        final var pendingDesiredNodes = randomList(0, 5, DesiredNodesTestCase::randomDesiredNode);

        final var updateDesiredNodesRequest = new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            1,
            concatLists(actualizedDesiredNodes, pendingDesiredNodes),
            false
        );
        updateDesiredNodes(updateDesiredNodesRequest);

        final var clusterState = clusterAdmin().prepareState().get().getState();
        DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect(clusterState, actualizedDesiredNodes, pendingDesiredNodes);

        // Stop some nodes, these shouldn't be actualized within the new desired node's history until they join back
        final var leavingNodeNames = randomSubsetOf(clusterNodeNames);
        for (String leavingNodeName : leavingNodeNames) {
            internalCluster().stopNode(leavingNodeName);
        }

        final var updateDesiredNodesWithNewHistoryRequest = new UpdateDesiredNodesRequest(
            randomAlphaOfLength(10),
            1,
            updateDesiredNodesRequest.getNodes(),
            false
        );
        final var response = updateDesiredNodes(updateDesiredNodesWithNewHistoryRequest);
        assertThat(response.hasReplacedExistingHistoryId(), is(equalTo(true)));

        final var updatedClusterState = clusterAdmin().prepareState().get().getState();
        final var latestDesiredNodes = DesiredNodes.latestFromClusterState(updatedClusterState);

        for (String clusterNodeName : clusterNodeNames) {
            final var desiredNode = latestDesiredNodes.find(clusterNodeName);
            assertThat(desiredNode.pending(), is(equalTo(leavingNodeNames.contains(clusterNodeName))));
        }
    }

    private UpdateDesiredNodesResponse updateDesiredNodes(UpdateDesiredNodesRequest request) {
        return client().execute(UpdateDesiredNodesAction.INSTANCE, request).actionGet();
    }

    public DesiredNode randomDesiredNodeWithName(String nodeName) {
        return randomDesiredNode(Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build());
    }
}
