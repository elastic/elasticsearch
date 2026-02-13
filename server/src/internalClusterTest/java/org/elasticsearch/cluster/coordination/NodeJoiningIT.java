/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.Collection;
import java.util.List;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class NodeJoiningIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // detect leader failover quickly
            .put(LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING.getKey(), 1)
            .put(LeaderChecker.LEADER_CHECK_INTERVAL_SETTING.getKey(), "100ms")
            .build();
    }

    public void testNodeJoinsCluster() {
        internalCluster().startNodes(3);
        String masterNodeName = internalCluster().getMasterName();
        int numberOfNodesOriginallyInCluster = internalCluster().clusterService(masterNodeName).state().getNodes().size();
        int numberOfMasterNodesOriginallyInCluster = internalCluster().clusterService(masterNodeName)
            .state()
            .nodes()
            .getMasterNodes()
            .size();
        List<String> namesOfDataNodesInOriginalCluster = getListOfDataNodeNamesFromCluster(masterNodeName);

        // Attempt to add new node
        String newNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(4);

        // Assert the new data node was added
        ClusterState state = internalCluster().clusterService(masterNodeName).state();
        assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
        assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
        assertEquals(numberOfMasterNodesOriginallyInCluster, state.nodes().getMasterNodes().size());

        List<String> namesOfDataNodesInNewCluster = getListOfDataNodeNamesFromCluster(masterNodeName);
        assertTrue(namesOfDataNodesInNewCluster.contains(newNodeName));
        for (String nodeName : namesOfDataNodesInOriginalCluster) {
            assertTrue(namesOfDataNodesInNewCluster.contains(nodeName));
        }
    }

    @TestLogging(reason = "test includes assertions about logging", value = "org.elasticsearch.cluster.coordination.NodeJoinExecutor:INFO")
    public void testNodeTriesToJoinClusterAndThenDifferentMasterIsElected() {
        List<String> nodeNames = internalCluster().startNodes(3);
        ensureStableCluster(3);
        String originalMasterNodeName = internalCluster().getMasterName();
        int numberOfNodesOriginallyInCluster = internalCluster().clusterService(originalMasterNodeName).state().getNodes().size();
        // Determine upfront who we want the next master to be
        final var newMasterNodeName = randomValueOtherThan(originalMasterNodeName, () -> randomFrom(nodeNames));

        // Ensure the logging is as expected
        try (var mockLog = MockLog.capture(NodeJoinExecutor.class)) {

            // Sets MockTransportService behaviour
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);

                if (mockTransportService.getLocalNode().getName().equals(newMasterNodeName) == false) {
                    List<String> listOfActionsToBlock = List.of(
                        // This forces the current master node to fail
                        PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                        // This disables pre-voting on all nodes except the new master, forcing it to win the election
                        StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME
                    );
                    blockActionNameOnMockTransportService(mockTransportService, listOfActionsToBlock);
                }
            }

            // We do not expect to see a WARN log about a node disconnecting (#ES-11449)
            addJoiningNodeDisconnectedWarnLogFalseExpectation(mockLog);

            // We haven't changed master nodes yet
            assertEquals(originalMasterNodeName, internalCluster().getMasterName());

            // Sends a node join request to the original master node. This will fail, and cause a master failover
            // startDataOnlyNode waits for the new node to be added, and this can only occur after a re-election
            String newNodeName = internalCluster().startDataOnlyNode();
            assertNotEquals(originalMasterNodeName, internalCluster().getMasterName());
            logger.info("New master is elected");

            // Assert all nodes have accepted N into their cluster state
            assertNewNodeIsInAllClusterStates(newNodeName);

            mockLog.assertAllExpectationsMatched();

            // Assert the new data node was added
            DiscoveryNodes discoveryNodes = internalCluster().clusterService().state().nodes();
            assertEquals(numberOfNodesOriginallyInCluster + 1, discoveryNodes.getSize());
            assertTrue(getListOfDataNodeNamesFromCluster(newMasterNodeName).contains(newNodeName));
        }
    }

    /*
        In this scenario, node N attempts to join a cluster, there is an election and the original master is re-elected.
        Node N should join the cluster, but it should not be disconnected (#ES-11449)
     */
    @TestLogging(reason = "test includes assertions about logging", value = "org.elasticsearch.cluster.coordination:INFO")
    @TestIssueLogging(
        value = "org.elasticsearch.cluster.coordination.NodeJoiningIT:DEBUG",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/136332"
    )
    public void testNodeTriesToJoinClusterAndThenSameMasterIsElected() {
        internalCluster().startNodes(3);
        ensureStableCluster(3);
        String masterNodeName = internalCluster().getMasterName();

        long originalTerm = getTerm(masterNodeName);
        int numberOfNodesOriginallyInCluster = internalCluster().clusterService(masterNodeName).state().getNodes().size();

        try (var mockLog = MockLog.capture(NodeJoinExecutor.class, MasterService.class, ClusterApplierService.class)) {
            for (String nodeName : internalCluster().getNodeNames()) {
                final var mockTransportService = MockTransportService.getInstance(nodeName);

                if (nodeName.equals(masterNodeName)) {
                    // This makes the master fail, forcing a re-election
                    blockActionNameOnMockTransportService(
                        mockTransportService,
                        List.of(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)
                    );

                    // Wait until the master has stepped down before removing the publishing ban
                    // This allows the master to be re-elected
                    ClusterServiceUtils.addTemporaryStateListener(internalCluster().clusterService(masterNodeName), clusterState -> {
                        DiscoveryNode currentMasterNode = clusterState.nodes().getMasterNode();
                        boolean hasMasterSteppedDown = currentMasterNode == null
                            || currentMasterNode.getName().equals(masterNodeName) == false;
                        if (hasMasterSteppedDown) {
                            logger.info("Master publishing ban removed");
                            mockTransportService.addSendBehavior(Transport.Connection::sendRequest);
                        }
                        return hasMasterSteppedDown;
                    });

                } else {
                    // This disables pre-voting on all nodes except the master, forcing it to win the election
                    blockActionNameOnMockTransportService(
                        mockTransportService,
                        List.of(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                    );
                }
            }

            // We expect the node join request to fail with a FailedToCommitClusterStateException
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "failed to commit cluster state",
                    MasterService.class.getCanonicalName(),
                    Level.WARN,
                    "failed to commit cluster state"
                )
            );

            /*
                We expect the cluster to reuse the connection to N and not disconnect it
                Therefore, this WARN log should not be thrown (#ES-11449)
             */
            addJoiningNodeDisconnectedWarnLogFalseExpectation(mockLog);

            // Before we add the new node, assert we haven't changed master nodes yet
            assertEquals(masterNodeName, internalCluster().getMasterName());

            // Sends a node join request to the original master node. This will fail, and cause a master failover
            logger.info("Sending node join request");
            String newNodeName = internalCluster().startDataOnlyNode();

            // Assert the master was re-elected
            assertEquals(masterNodeName, internalCluster().getMasterName());
            assertTrue(originalTerm < getTerm(masterNodeName));

            // Assert all nodes have accepted N into their cluster state
            assertNewNodeIsInAllClusterStates(newNodeName);

            // If the WARN log was thrown, then the connection to N was disconnected so fail the test
            mockLog.assertAllExpectationsMatched();

            // Assert the new data node was added
            DiscoveryNodes discoveryNodes = internalCluster().clusterService().state().nodes();
            assertEquals(numberOfNodesOriginallyInCluster + 1, discoveryNodes.getSize());
            assertTrue(getListOfDataNodeNamesFromCluster(masterNodeName).contains(newNodeName));
        }
    }

    private long getTerm(String masterNodeName) {
        return internalCluster().clusterService(masterNodeName).state().coordinationMetadata().term();
    }

    private void assertNewNodeIsInAllClusterStates(String newNodeName) {
        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            assertTrue(clusterService.state().nodes().getAllNodes().stream().map(DiscoveryNode::getName).toList().contains(newNodeName));
        }
    }

    private List<String> getListOfDataNodeNamesFromCluster(String nodeName) {
        return internalCluster().clusterService(nodeName)
            .state()
            .getNodes()
            .getDataNodes()
            .values()
            .stream()
            .map(DiscoveryNode::getName)
            .toList();
    }

    private void addJoiningNodeDisconnectedWarnLogFalseExpectation(MockLog mockLog) {
        mockLog.addExpectation(
            new MockLog.UnseenEventExpectation(
                "warn message with troubleshooting link",
                "org.elasticsearch.cluster.coordination.NodeJoinExecutor",
                Level.WARN,
                "*"
            )
        );
    }

    private void blockActionNameOnMockTransportService(MockTransportService mockTransportService, List<String> actionNamesToBlock) {
        mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (actionNamesToBlock.contains(action)) {
                throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });
    }
}
