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
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NodeJoiningIT extends MasterElectionTestCase {

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
        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            ensureSufficientMasterEligibleNodes();
            String masterNodeName = internalCluster().getMasterName();
            DiscoveryNode masterNode = internalCluster().clusterService(masterNodeName).state().nodes().getMasterNode();
            int numberOfNodesOriginallyInCluster = internalCluster().clusterService(masterNodeName).state().getNodes().size();
            List<String> namesOfDataNodesInOriginalCluster = getListOfDataNodeNamesFromCluster(masterNodeName);

            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(NodeJoinExecutor.class)) {
                assert masterNode != null;
                String expectedNewNodeAsString = generateNodeDescriptionForNewDiscoveryNode(numberOfNodesOriginallyInCluster, masterNode);

                // We expect to see a node join message from the new node
                addNodeJoinExpectation(mockLog, expectedNewNodeAsString);

                // Attempt to add new node
                String newNodeName = internalCluster().startDataOnlyNode();

                mockLog.assertAllExpectationsMatched();

                // Assert the new data node was added
                ClusterState state = internalCluster().clusterService(masterNodeName).state();
                assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
                assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
                assertEquals(5, state.nodes().getMasterNodes().size());

                List<String> namesOfDataNodesInNewCluster = getListOfDataNodeNamesFromCluster(masterNodeName);
                assertTrue(namesOfDataNodesInNewCluster.contains(newNodeName));
                for (String nodeName : namesOfDataNodesInOriginalCluster) {
                    assertTrue(namesOfDataNodesInNewCluster.contains(nodeName));
                }
            }
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(cleanupTasks));
        }
    }

    @TestLogging(
        reason = "test includes assertions about logging",
        value = "org.elasticsearch.cluster.coordination.NodeJoinExecutor:WARN,org.elasticsearch.cluster.coordination.NodeJoinExecutor:INFO"
    )
    public void testNodeTriesToJoinClusterAndThenDifferentMasterIsElected() {
        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            // Set up master nodes and determine upfront who we want the next master to be
            final var newMasterNodeName = ensureSufficientMasterEligibleNodes();
            String originalMasterNodeName = internalCluster().getMasterName();
            int numberOfNodesOriginallyInCluster = internalCluster().clusterService(originalMasterNodeName).state().getNodes().size();
            List<String> namesOfDataNodesInOriginalCluster = getListOfDataNodeNamesFromCluster(originalMasterNodeName);
            DiscoveryNode masterNode = internalCluster().clusterService(originalMasterNodeName).state().getNodes().getMasterNode();

            // A CountDownLatch that only gets decremented when the first master acknowledges the second master
            final var previousMasterKnowsNewMasterIsElectedLatch = configureElectionLatchForNewMaster(newMasterNodeName, cleanupTasks);

            // Sets MockTransportService behaviour
            for (final var transportService : internalCluster().getInstances(TransportService.class)) {
                final var mockTransportService = asInstanceOf(MockTransportService.class, transportService);
                cleanupTasks.add(mockTransportService::clearAllRules);

                if (mockTransportService.getLocalNode().getName().equals(newMasterNodeName) == false) {
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        if (
                        // This disables pre-voting on all nodes except the new master, forcing it to win the election
                        action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)
                            // This forces the current master node to fail
                            || action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });
                }
            }

            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(NodeJoinExecutor.class)) {
                assert masterNode != null;
                String expectedNewNodeAsString = generateNodeDescriptionForNewDiscoveryNode(numberOfNodesOriginallyInCluster, masterNode);

                // We expect to see a node join message from the new node
                addNodeJoinExpectation(mockLog, expectedNewNodeAsString);

                // We do not expect to see the WARN log
                addJoiningNodeDisconnectedWarnLogExpectation(mockLog, expectedNewNodeAsString);

                // We haven't changed master nodes yet
                assertEquals(originalMasterNodeName, internalCluster().getMasterName());

                // Sends a node join request to the original master node. This will fail, and cause a master failover
                String newNodeName = internalCluster().startDataOnlyNode();

                // Wait until the old master has acknowledged the new master's election
                safeAwait(previousMasterKnowsNewMasterIsElectedLatch);
                assertNotEquals(originalMasterNodeName, internalCluster().getMasterName());
                logger.info("New master is elected");

                mockLog.assertAllExpectationsMatched();

                // Assert the new data node was added
                ClusterState state = internalCluster().clusterService().state();
                assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
                assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
                assertEquals(5, state.nodes().getMasterNodes().size());

                List<String> namesOfDataNodesInNewCluster = getListOfDataNodeNamesFromCluster(newMasterNodeName);
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
        value = "org.elasticsearch.cluster.coordination.NodeJoinExecutor:WARN,"
            + "org.elasticsearch.cluster.coordination.NodeJoinExecutor:INFO,"
            + "org.elasticsearch.cluster.coordination.MasterService:WARN,"
            + "org.elasticsearch.cluster.coordination.MasterService:INFO,"
            + "org.elasticsearch.cluster.coordination.ClusterApplierService:WARN"
    )
    public void testNodeTriesToJoinClusterAndThenSameMasterIsElected_DoesNotIncludeWarnLog() {
        final var cleanupTasks = new ArrayList<Releasable>();

        try {
            ensureSufficientMasterEligibleNodes();
            String masterNodeName = internalCluster().getMasterName();
            DiscoveryNode masterNode = internalCluster().clusterService(masterNodeName).state().nodes().getMasterNode();

            long originalTerm = internalCluster().clusterService(masterNodeName).state().coordinationMetadata().term();
            long originalVersion = internalCluster().clusterService(masterNodeName).state().version();
            int numberOfNodesOriginallyInCluster = internalCluster().clusterService(masterNodeName).state().getNodes().size();
            List<String> namesOfDataNodesInOriginalCluster = getListOfDataNodeNamesFromCluster(masterNodeName);
            String[] namesOfAllNodesInOriginalCluster = internalCluster().getNodeNames();

            final var masterNodeTransportService = MockTransportService.getInstance(masterNodeName);
            cleanupTasks.add(masterNodeTransportService::clearAllRules);

            // Mocks behaviour to force the master to step down
            masterNodeTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                // This makes the master fail, forcing a re-election
                if (action.equals(PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME)) {
                    throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            });

            // Latch to remove publishing ban to allow re-election
            CountDownLatch publishingBanRemovedLatch = removeMockTransportServicePublishBanWhenMasterHasSteppedDown(
                masterNodeName,
                masterNodeTransportService,
                cleanupTasks
            );

            // A CountDownLatch that only gets decremented when the first master node is re-elected
            final var masterKnowsItHasBeenReElectedLatch = configureElectionLatchForReElectedMaster(
                masterNodeName,
                originalTerm,
                cleanupTasks
            );

            for (String nodeName : internalCluster().getNodeNames()) {
                final var mockTransportService = MockTransportService.getInstance(nodeName);
                cleanupTasks.add(mockTransportService::clearAllRules);

                if (nodeName.equals(masterNodeName) == false) {
                    mockTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
                        // This disables pre-voting on all nodes except the master, forcing it to win the election
                        if (action.equals(StatefulPreVoteCollector.REQUEST_PRE_VOTE_ACTION_NAME)) {
                            throw new ElasticsearchException("[{}] for [{}] denied", action, connection.getNode());
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });
                }
            }

            // Ensure the logging is as expected
            try (var mockLog = MockLog.capture(NodeJoinExecutor.class, MasterService.class, ClusterApplierService.class)) {
                long firstNodeJoinVersion = originalVersion + 1;
                assert masterNode != null;
                String expectedNewNodeAsString = generateNodeDescriptionForNewDiscoveryNode(numberOfNodesOriginallyInCluster, masterNode);

                /*
                     We expect to see a node join event as the master tries to process the join,
                     but cannot commit the cluster state due to a lack of quorum.
                     This means the join is not successful in this term.
                 */
                addMasterServiceUpdateTaskNodeJoinExpectation(mockLog, expectedNewNodeAsString, originalTerm, firstNodeJoinVersion);

                // The node join fails
                addFailedToCommitClusterStateVersionExpectation(mockLog, expectedNewNodeAsString, firstNodeJoinVersion);

                /*
                    We expect the cluster to reuse the connection to N and not disconnect it
                    Therefore, this WARN log should not be thrown
                 */
                addJoiningNodeDisconnectedWarnLogExpectation(mockLog, expectedNewNodeAsString);

                /*
                    We expect node N to join the cluster.
                    However, we expect no explicit `node-join: [{node_s5} ...]` log to be emitted by the master in this new term
                    as the join is processed as part of the new election and cluster state publication, rather than a separate event
                 */
                addNodeJoinProcessedDuringNewElectionAndClusterStatePublicationExpectation(mockLog, expectedNewNodeAsString);

                // Before we add the new node, assert we haven't changed master nodes yet
                assertEquals(masterNodeName, internalCluster().getMasterName());

                // Sends a node join request to the original master node. This will fail, and cause a master failover
                logger.info("Sending node join request");
                String newNodeName = internalCluster().startDataOnlyNode();

                // Wait until the master has stepped down before removing the publishing ban
                safeAwait(publishingBanRemovedLatch);
                logger.info("Master publishing ban removed");

                // Wait until the master acknowledges its re-election. The master is only re-elected once it's publishing ban is lifted
                safeAwait(masterKnowsItHasBeenReElectedLatch);
                assertEquals(masterNodeName, internalCluster().getMasterName());
                logger.info("Master has been re-elected");

                try {
                    // Await for N to be in the cluster state of all nodes
                    for (String nodeName : namesOfAllNodesInOriginalCluster) {
                        ClusterServiceUtils.awaitClusterState(
                            logger,
                            clusterState -> clusterState.nodes().nodeExistsWithName(newNodeName),
                            internalCluster().clusterService(nodeName)
                        );
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

                // If the WARN log was thrown, then the connection to N was disconnected so fail the test
                mockLog.assertAllExpectationsMatched();

                // Assert the new data node was added
                ClusterState state = internalCluster().clusterService(masterNodeName).state();
                assertEquals(numberOfNodesOriginallyInCluster + 1, state.nodes().getSize());
                assertEquals(namesOfDataNodesInOriginalCluster.size() + 1, state.nodes().getDataNodes().size());
                assertEquals(5, state.nodes().getMasterNodes().size());

                List<String> namesOfDataNodesInNewCluster = getListOfDataNodeNamesFromCluster(masterNodeName);
                assertTrue(namesOfDataNodesInNewCluster.contains(newNodeName));
                for (String nodeName : namesOfDataNodesInOriginalCluster) {
                    assertTrue(namesOfDataNodesInNewCluster.contains(nodeName));
                }
            }
        } finally {
            Releasables.closeExpectNoException(Releasables.wrap(cleanupTasks));
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

    private void addNodeJoinExpectation(MockLog mockLog, String expectedNewNodeAsString) {
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

                Pattern pattern = Pattern.compile("node-join: \\[" + expectedNewNodeAsString + "] " + "with reason \\[joining]");
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
    }

    private void addMasterServiceUpdateTaskNodeJoinExpectation(MockLog mockLog, String expectedNewNodeAsString, long term, long version) {
        mockLog.addExpectation(new MockLog.LoggingExpectation() {
            boolean matched = false;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() != Level.INFO) {
                    return;
                }
                if (event.getLoggerName().equals(MasterService.class.getCanonicalName()) == false) {
                    return;
                }

                Pattern pattern = Pattern.compile(
                    "node-join\\["
                        + expectedNewNodeAsString
                        + " joining],"
                        + " term: "
                        + term
                        + ","
                        + " version: "
                        + version
                        + ","
                        + " delta: added \\{"
                        + expectedNewNodeAsString
                        + "}"
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
    }

    private void addFailedToCommitClusterStateVersionExpectation(MockLog mockLog, String expectedNewNodeAsString, long version) {
        mockLog.addExpectation(new MockLog.LoggingExpectation() {
            boolean matched = false;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() != Level.WARN) {
                    return;
                }
                if (event.getLoggerName().equals(MasterService.class.getCanonicalName()) == false) {
                    return;
                }

                Pattern pattern = Pattern.compile(
                    "failing \\[node-join\\["
                        + expectedNewNodeAsString
                        + " joining]]: failed to commit cluster state version \\["
                        + version
                        + "]"
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
    }

    private void addJoiningNodeDisconnectedWarnLogExpectation(MockLog mockLog, String expectedNewNodeAsString) {
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
                Pattern pattern = Pattern.compile(
                    "node-join: \\["
                        + expectedNewNodeAsString
                        + "] "
                        + "with reason \\[joining, removed \\["
                        + regexToMatchAnyCharacterExceptClosingBrace
                        + "] ago with reason \\[disconnected]]; "
                        + "for troubleshooting guidance, see "
                        + "https://www.elastic.co/docs/troubleshoot/elasticsearch/troubleshooting-unstable-cluster\\?version=master"
                );
                Matcher matcher = pattern.matcher(event.getMessage().getFormattedMessage());

                if (matcher.find()) {
                    matched = true;
                }
            }

            @Override
            public void assertMatched() {
                assertFalse(matched);
            }
        });
    }

    private void addNodeJoinProcessedDuringNewElectionAndClusterStatePublicationExpectation(
        MockLog mockLog,
        String expectedNewNodeAsString
    ) {
        mockLog.addExpectation(new MockLog.LoggingExpectation() {
            boolean matched = false;

            @Override
            public void match(LogEvent event) {
                if (event.getLevel() != Level.INFO) {
                    return;
                }
                if (event.getLoggerName().equals(ClusterApplierService.class.getCanonicalName()) == false) {
                    return;
                }

                Pattern pattern = Pattern.compile("added \\{" + expectedNewNodeAsString + "}");
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
    }

    private String generateNodeDescriptionForNewDiscoveryNode(int numberOfNodesOriginallyInCluster, DiscoveryNode masterNode) {
        // Nodes are named `node_s0`, `node_s1` etc ...
        // Therefore, if there are N nodes in the cluster, named `node_s0` ... `node_sN-1`, N+1 will be named `node_sN`
        String newNodeName = "node_s" + numberOfNodesOriginallyInCluster;
        String regexToMatchAnyCharacterExceptClosingBrace = "([^}]+)";

        return "\\{"
            + newNodeName
            + "}"
            + "\\{"
            + regexToMatchAnyCharacterExceptClosingBrace
            + "}"
            + "\\{"
            + regexToMatchAnyCharacterExceptClosingBrace
            + "}"
            + "\\{"
            + newNodeName
            + "}"
            + "\\{"
            + masterNode.getHostAddress()
            + "}"
            + "\\{"
            + masterNode.getHostAddress()
            + ":\\d+}"
            + "\\{d}"
            + "\\{"
            + masterNode.getVersion()
            + "}"
            + "\\{"
            + regexToMatchAnyCharacterExceptClosingBrace
            + "}";
    }

    /**
     * Removes all custom mockTransportService.addSendBehavior so that the original master node can run for election
     *
     * @param masterNodeName The name of the current master node
     * @param mockTransportService The transport service to remove the `addSendBehavior` from
     * @param cleanupTasks The list of cleanup tasks
     */
    protected CountDownLatch removeMockTransportServicePublishBanWhenMasterHasSteppedDown(
        String masterNodeName,
        MockTransportService mockTransportService,
        List<Releasable> cleanupTasks
    ) {
        CountDownLatch latch = new CountDownLatch(1);
        ClusterStateApplier newMasterMonitor = event -> {
            DiscoveryNode masterNode = event.state().nodes().getMasterNode();
            if (masterNode == null || masterNode.getName().equals(masterNodeName) == false) {
                // Remove the publishing ban
                mockTransportService.addSendBehavior(Transport.Connection::sendRequest);
                latch.countDown();
            }
        };
        ClusterService masterClusterService = internalCluster().getInstance(ClusterService.class, masterNodeName);
        masterClusterService.addStateApplier(newMasterMonitor);
        cleanupTasks.add(() -> masterClusterService.removeApplier(newMasterMonitor));
        return latch;
    }
}
