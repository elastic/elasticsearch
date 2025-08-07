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
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NodeJoiningMasterElectionIT extends MasterElectionTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(
            super.nodePlugins(),
            MockTransportService.TestPlugin.class
        );
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

    @TestLogging(
        reason = "test includes assertions about logging",
        value = "org.elasticsearch.cluster.coordination.NodeJoinExecutor:WARN,org.elasticsearch.cluster.coordination.NodeJoinExecutor:INFO"
    )
    public void testNodeTriesToJoinClusterAndThenDifferentMasterIsElected() {
        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            // Set up master nodes and determine upfront who we want the next master to be
            final var newMaster = ensureSufficientMasterEligibleNodes();

            String originalMasterName = internalCluster().getMasterName();
            int numberOfNodesOriginallyInCluster = internalCluster().clusterService().state().getNodes().size();
            List<String> namesOfDataNodesInOriginalCluster = internalCluster()
                .clusterService()
                .state()
                .getNodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getName)
                .toList();
            DiscoveryNode masterNode = internalCluster().clusterService().state().getNodes().getMasterNode();

            // A CountDownLatch that only gets decremented when the first master acknowledges the second master
            final var previousMasterKnowsNewMasterIsElectedLatch = configureElectionLatchForNewMaster(newMaster, cleanupTasks);

            // Sets MockTransportService behaviour
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                cleanupTasks.add(mockTransportService::clearAllRules);

                if (mockTransportService.getLocalNode().getName().equals(newMaster) == false) {
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        if (
                            // This disables pre-voting on all nodes except the new master, forcing it to win the election
                            action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                            // This forces the current master node to fail
                            || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)
                        ) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });
                }
            }

            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(NodeJoinExecutor.class)) {

                // This matches with the node join message from the new node only
                mockLog.addExpectation(new MockLog.LoggingExpectation() {
                    boolean matched = false;

                    @Override
                    public void match(LogEvent event) {
                        if (event.getLevel() != Level.INFO) {
                            return;
                        }
                        if (event.getLoggerName().equals(NodeJoinExecutor.class.getCanonicalName()) == false) {
                            return;
                        }

                        // Since we declare this log message before we add a node, we need to predict what the node description will be
                        assert masterNode != null;
                        Pattern pattern = Pattern.compile(
                            "node-join: \\["
                                + generateNodeDescriptionForNewNode(numberOfNodesOriginallyInCluster, masterNode)
                                + "] "
                                + "with reason \\[joining]"
                        );
                        Matcher matcher = pattern.matcher(event.getMessage().getFormattedMessage());

                        if (matcher.find()) {
                            matched = true;
                        }
                    }

                    @Override
                    public void assertMatched() {
                        assertTrue(matched);
                    }
                });

                // We haven't changed master nodes yet
                assertEquals(originalMasterName, internalCluster().getMasterName());

                // Sends a node join request to the original master node. This will fail, and cause a master failover
                String newNodeName = internalCluster().startDataOnlyNode();

                // Wait until the old master has acknowledged the new master's election
                safeAwait(previousMasterKnowsNewMasterIsElectedLatch);
                assertNotEquals(originalMasterName, internalCluster().getMasterName());
                logger.info("New master is elected");

                mockLog.assertAllExpectationsMatched();

                // Assert the new data node was added
                ClusterState state = internalCluster().clusterService().state();
                assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
                assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
                assertEquals(5, state.nodes().getMasterNodes().size());

                List<String> namesOfDataNodesInNewCluster = state.nodes()
                    .getDataNodes()
                    .values()
                    .stream()
                    .map(DiscoveryNode::getName)
                    .toList();
                assertTrue(namesOfDataNodesInNewCluster.contains(newNodeName));
                for (String nodeName : namesOfDataNodesInOriginalCluster) {
                    assertTrue(namesOfDataNodesInNewCluster.contains(nodeName));
                }
            }
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(cleanupTasks));
        }
    }

    // Tests whether a WARN log is thrown when a node attempts to join a cluster, and then the same master node is re-elected (#126192)
    @TestLogging(
        reason = "test includes assertions about logging",
        value = "org.elasticsearch.cluster.coordination.NodeJoinExecutor:WARN"
    )
    public void testNodeTriesToJoinClusterAndThenSameMasterIsElected() {
        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            ensureSufficientMasterEligibleNodes();
            DiscoveryNode masterNode = internalCluster().clusterService().state().nodes().getMasterNode();
            String originalMasterName = internalCluster().getMasterName();

            long originalTerm = internalCluster().clusterService().state().coordinationMetadata().term();
            int numberOfNodesOriginallyInCluster = internalCluster().clusterService().state().getNodes().size();
            List<String> namesOfDataNodesInOriginalCluster = internalCluster()
                .clusterService()
                .state()
                .getNodes()
                .getDataNodes()
                .values()
                .stream()
                .map(DiscoveryNode::getName)
                .toList();

            // A CountDownLatch that only gets decremented when the first master node is re-elected
            final var masterKnowsItHasBeenReElectedLatch = configureElectionLatchForReElectedMaster(originalMasterName, originalTerm, cleanupTasks);

            // A list of CountDownLatches, one for each node, that only gets decremented when it recieves a cluster state update containing the new node N
            List<CountDownLatch> newNodeIsPresentInClusterStateLatchList = new ArrayList<>();

            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                cleanupTasks.add(mockTransportService::clearAllRules);

                String nodeName = mockTransportService.getLocalNode().getName();
                newNodeIsPresentInClusterStateLatchList.add(waitUntilNewNodeIsPresentInClusterStateUpdate(nodeName, numberOfNodesOriginallyInCluster, cleanupTasks));

                if (nodeName.equals(originalMasterName)) {
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        // This makes the master fail, forcing a re-election
                        if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });

                    // This removes the PUBLISH_STATE_ACTION_NAME mocking set above once we have triggered an election to allow the master node to be re-elected
                    removeMockTransportServicePublishBanWhenMasterHasSteppedDown(originalMasterName, originalTerm, mockTransportService, cleanupTasks);
                } else {
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        if (
                            // This disables pre-voting on all nodes except the master, forcing it to win the election
                            action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                                || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)
                        ) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });
                }
            }

            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(NodeJoinExecutor.class)) {

                // This tries to match against the WARN log emitted when a node attempts to rejoin a cluster without restarting
                mockLog.addExpectation(new MockLog.LoggingExpectation() {
                    boolean matched = false;

                    @Override
                    public void match(LogEvent event) {
                        if (event.getLevel() != Level.WARN) {
                            return;
                        }
                        if (event.getLoggerName().equals(NodeJoinExecutor.class.getCanonicalName()) == false) {
                            return;
                        }

                        String regexToMatchAnyCharacterExceptClosingBrace = "([^}]+)";
                        assert masterNode != null;
                        // Since we declare this log message before we add a node, we need to predict what the node description will be
                        Pattern pattern = Pattern.compile(
                            "node-join: \\["
                                + generateNodeDescriptionForNewNode(numberOfNodesOriginallyInCluster, masterNode)
                                + "] "
                                + "with reason \\[joining, removed \\["
                                + regexToMatchAnyCharacterExceptClosingBrace
                                + "] ago with reason \\[disconnected]]; "
                                + "for troubleshooting guidance, see https://www.elastic.co/docs/troubleshoot/elasticsearch/troubleshooting-unstable-cluster\\?version=master"
                        );
                        Matcher matcher = pattern.matcher(event.getMessage().getFormattedMessage());

                        if (matcher.find()) {
                            matched = true;
                        }
                    }

                    @Override
                    public void assertMatched() {
                        assertTrue(matched);
                    }
                });

                // We haven't changed master nodes yet
                assertEquals(originalMasterName, internalCluster().getMasterName());

                // Sends a node join request to the original master node. This will fail, and cause a master failover
                String newNodeName = internalCluster().startDataOnlyNode();


                // Wait until the master acknowledges its re-election. The master is only re-elected once it's publishing ban is lifted
                safeAwait(masterKnowsItHasBeenReElectedLatch);
                assertEquals(originalMasterName, internalCluster().getMasterName());

                logger.info("Master has been re-elected");

                // Wait for the cluster state update containing the new node N to propagate to all nodes. This gives times for the WARN log to be emitted
                for (CountDownLatch countDownLatch : newNodeIsPresentInClusterStateLatchList) {
                    safeAwait(countDownLatch);
                }

                mockLog.assertAllExpectationsMatched();

                // Assert the new data node was added
                ClusterState state = internalCluster().clusterService().state();
                assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
                assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
                assertEquals(5, state.nodes().getMasterNodes().size());

                List<String> namesOfDataNodesInNewCluster = state.nodes()
                    .getDataNodes()
                    .values()
                    .stream()
                    .map(DiscoveryNode::getName)
                    .toList();
                assertTrue(namesOfDataNodesInNewCluster.contains(newNodeName));
                for (String nodeName : namesOfDataNodesInOriginalCluster) {
                    assertTrue(namesOfDataNodesInNewCluster.contains(nodeName));
                }
            }
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(cleanupTasks));
        }
    }

    private String generateNodeDescriptionForNewNode(int numberOfNodesOriginallyInCluster, DiscoveryNode masterNode) {
        // Nodes are named `node_s0`, `node_s1` etc ...
        // Therefore, if there are N nodes in the cluster, named `node_s0` ... `node_sN-1`, the next will be `node_sN`
        String newNodeName = "node_s" + numberOfNodesOriginallyInCluster;
        String regexToMatchAnyCharacterExceptClosingBrace = "([^}]+)";

        return "\\{" + newNodeName + "}"
            + "\\{" + regexToMatchAnyCharacterExceptClosingBrace + "}"
            + "\\{" + regexToMatchAnyCharacterExceptClosingBrace + "}"
            + "\\{" + newNodeName + "}"
            + "\\{" + masterNode.getHostAddress() + "}"
            + "\\{" + masterNode.getHostAddress() + ":\\d+}"
            + "\\{d}"
            + "\\{" + masterNode.getVersion() + "}"
            + "\\{" + regexToMatchAnyCharacterExceptClosingBrace + "}";
    }

    /**
     * Removes all custom mockTransportService.addSendBehavior so that the original master node can run for election
     *
     * @param masterNodeName The name of the current master node
     * @param originalTerm The term the current master node was first elected
     * @param cleanupTasks The list of cleanup tasks
     */
    protected void removeMockTransportServicePublishBanWhenMasterHasSteppedDown(String masterNodeName, long originalTerm, MockTransportService mockTransportService, List<Releasable> cleanupTasks) {
        ClusterStateApplier newMasterMonitor = event -> {
            DiscoveryNode masterNode = event.state().nodes().getMasterNode();
            long currentTerm = event.state().coordinationMetadata().term();
            if (masterNode == null && currentTerm == originalTerm) {
                // Remove the publishing ban
                mockTransportService.addSendBehavior(Transport.Connection::sendRequest);
            }
        };
        ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        masterClusterService.addStateApplier(newMasterMonitor);
        cleanupTasks.add(() -> masterClusterService.removeApplier(newMasterMonitor));
    }

    /**
     * Waits until the new node N is in the cluster
     * As per #126192, the new node is:
     * 1. Included in cluster state update (T, V) sent to all nodes.
     * 2. Only M accepts the publication.
     * 3. After receiving no acknowledgements, M steps down
     * 4. M is re-elected. M sends out a new cluster state, (T+1, V+1) still with N included. Now N is added to all nodes in the cluster
     * 5. The connection to N was closed when (T, V) failed
     * 6. A node-left task is enqueued. This is cluster state update (T+1, V+2). All nodes have N removed from the cluster
     * 7. Node N rejoins. This is cluster state update (T+1, V+3).
     * 8. Hence, we wait until N is added, removed, and then added again before releasing the latch
     *
     * @param nodeName The name of the current node
     * @param originalNumberOfNodes The number of nodes the cluster had before we attempted to add node N
     * @param cleanupTasks The list of cleanup tasks
     */
    protected CountDownLatch waitUntilNewNodeIsPresentInClusterStateUpdate(String nodeName, int originalNumberOfNodes, List<Releasable> cleanupTasks) {
        final AtomicBoolean nodeHasBeenAddedFirstTime = new AtomicBoolean(false);
        final AtomicBoolean addedNodeHasBeenRemoved = new AtomicBoolean(false);
        final var nodeHasBeenAddedLatch = new CountDownLatch(1);
        ClusterStateApplier newMasterMonitor = event -> {
            long currentNumberOfNodes = event.state().nodes().getAllNodes().size();

            // First time the node has been added
            if (!nodeHasBeenAddedFirstTime.get() && currentNumberOfNodes > originalNumberOfNodes) {
                nodeHasBeenAddedFirstTime.set(true);
            }
            // The node has been added and is now removed
            else if (nodeHasBeenAddedFirstTime.get() && !addedNodeHasBeenRemoved.get() && currentNumberOfNodes <= originalNumberOfNodes) {
                addedNodeHasBeenRemoved.set(true);
            }
            // The node has been added again, throwing the WARN log
            else if (nodeHasBeenAddedFirstTime.get() && addedNodeHasBeenRemoved.get() && currentNumberOfNodes > originalNumberOfNodes) {
                nodeHasBeenAddedLatch.countDown();
            }
        };
        ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, nodeName);
        masterClusterService.addStateApplier(newMasterMonitor);
        cleanupTasks.add(() -> masterClusterService.removeApplier(newMasterMonitor));
        return nodeHasBeenAddedLatch;
    }
}
