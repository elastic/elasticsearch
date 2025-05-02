/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.Map;
import java.util.Set;

public class AllocationBalancingRoundSummaryServiceTests extends ESTestCase {
    private static final Logger logger = LogManager.getLogger(AllocationBalancingRoundSummaryServiceTests.class);

    private static final String BALANCING_SUMMARY_MSG_PREFIX = "Balancing round summaries:*";

    private static final Map<String, BalancingRoundSummary.NodesWeightsChanges> NODE_NAME_TO_WEIGHT_CHANGES = Map.of(
        "node1",
        new BalancingRoundSummary.NodesWeightsChanges(
            new DesiredBalanceMetrics.NodeWeightStats(1L, 2, 3, 4),
            new BalancingRoundSummary.NodeWeightsDiff(1, 2, 3, 4)
        ),
        "node2",
        new BalancingRoundSummary.NodesWeightsChanges(
            new DesiredBalanceMetrics.NodeWeightStats(1L, 2, 3, 4),
            new BalancingRoundSummary.NodeWeightsDiff(1, 2, 3, 4)
        )
    );

    final DiscoveryNode DUMMY_NODE = new DiscoveryNode("node1Name", "node1Id", "eph-node1", "abc", "abc", null, Map.of(), Set.of(), null);
    final DiscoveryNode SECOND_DUMMY_NODE = new DiscoveryNode(
        "node2Name",
        "node2Id",
        "eph-node2",
        "def",
        "def",
        null,
        Map.of(),
        Set.of(),
        null
    );

    final String INDEX_NAME = "index";
    final String INDEX_UUID = "_indexUUID_";

    final Settings enabledSummariesSettings = Settings.builder()
        .put(AllocationBalancingRoundSummaryService.ENABLE_BALANCER_ROUND_SUMMARIES_SETTING.getKey(), true)
        .build();
    final Settings disabledDefaultEmptySettings = Settings.builder().build();

    ClusterSettings enabledClusterSettings = new ClusterSettings(enabledSummariesSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    ClusterSettings disabledDefaultEmptyClusterSettings = new ClusterSettings(
        disabledDefaultEmptySettings,
        ClusterSettings.BUILT_IN_CLUSTER_SETTINGS
    );

    // Construction parameters for the service.

    DeterministicTaskQueue deterministicTaskQueue;
    ThreadPool testThreadPool;

    @Before
    public void setUpThreadPool() {
        deterministicTaskQueue = new DeterministicTaskQueue();
        testThreadPool = deterministicTaskQueue.getThreadPool();
    }

    /**
     * Test that the service is disabled and no logging occurs when
     * {@link AllocationBalancingRoundSummaryService#ENABLE_BALANCER_ROUND_SUMMARIES_SETTING} defaults to false.
     */
    public void testServiceDisabledByDefault() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, disabledDefaultEmptyClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add a summary and check it is not logged.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 5, 0, 0, 6, 2));
            service.verifyNumberOfSummaries(0); // when summaries are disabled, summaries are not retained when added.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );

            if (deterministicTaskQueue.hasDeferredTasks()) {
                deterministicTaskQueue.advanceTime();
            }
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    public void testEnabledService() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add a summary and check the service logs report on it.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 3, 0, 0, 2, 0));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);

            /**
             * Add a second summary, check for more logging.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 200, 9, 0, 0, 0, 0));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging a second time",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * The service should combine multiple summaries together into a single report when multiple summaries were added since the last report.
     */
    public void testCombinedSummary() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 0, 0, 0, 0, 0));
            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 100, 0, 0, 0, 0, 0));
            service.verifyNumberOfSummaries(2);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging of combined summaries",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*150*"
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * The service shouldn't log anything when there haven't been any summaries added since the last report.
     */
    public void testNoSummariesToReport() {
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, enabledClusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * First add some summaries to report, ensuring that the logging is active.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 0, 0, 0, 0, 0));
            service.verifyNumberOfSummaries(1);
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Running balancer summary logging of combined summaries",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    BALANCING_SUMMARY_MSG_PREFIX
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);

            /**
             * Now check that there are no further log messages because there were no further summaries added.
             */

            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "No balancer round summary to log",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );

            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * Test that the service is disabled by setting {@link AllocationBalancingRoundSummaryService#ENABLE_BALANCER_ROUND_SUMMARIES_SETTING}
     * to false.
     */
    public void testEnableAndThenDisableService() {
        var disabledSettingsUpdate = Settings.builder()
            .put(AllocationBalancingRoundSummaryService.ENABLE_BALANCER_ROUND_SUMMARIES_SETTING.getKey(), false)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(enabledSummariesSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        var service = new AllocationBalancingRoundSummaryService(testThreadPool, clusterSettings);

        try (var mockLog = MockLog.capture(AllocationBalancingRoundSummaryService.class)) {
            /**
             * Add some summaries, but then disable the service before logging occurs. Disabling the service should drain and discard any
             * summaries waiting to be reported.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 0, 0, 0, 0, 0));
            service.verifyNumberOfSummaries(1);

            clusterSettings.applySettings(disabledSettingsUpdate);
            service.verifyNumberOfSummaries(0);

            /**
             * Verify that any additional summaries are not retained, since the service is disabled.
             */

            service.addBalancerRoundSummary(new BalancingRoundSummary(NODE_NAME_TO_WEIGHT_CHANGES, 50, 0, 0, 0, 0, 0));
            service.verifyNumberOfSummaries(0);

            // Check that the service never logged anything.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "Running balancer summary logging",
                    AllocationBalancingRoundSummaryService.class.getName(),
                    Level.INFO,
                    "*"
                )
            );
            deterministicTaskQueue.advanceTime();
            deterministicTaskQueue.runAllRunnableTasks();
            mockLog.awaitAllExpectationsMatched();
            service.verifyNumberOfSummaries(0);
        }
    }

    /**
     * Tests the {@link AllocationBalancingRoundSummaryService#createBalancerRoundSummary(DesiredBalance, DesiredBalance)} logic.
     */
    public void testCreateBalancerRoundSummary() {
        // Initial desired balance allocations and node weights.
        DesiredBalance firstDesiredBalance = new DesiredBalance(
            1,
            // The shard assignments and node weights don't make sense together, but for summary purposes the first determines the summary's
            // number of shards moved, and the second the weight changes: the summary service doesn't need them to make sense together
            // because it looks at them separately. They do have to make sense individually across balancing rounds.
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of("a", "b"), 2, 0, 0)),
            Map.of(DUMMY_NODE, new DesiredBalanceMetrics.NodeWeightStats(10, 20, 30, 40)),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );
        // Move two shards and change the node weights.
        DesiredBalance secondDesiredBalance = new DesiredBalance(
            1,
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of("c", "d"), 2, 0, 0)),
            Map.of(DUMMY_NODE, new DesiredBalanceMetrics.NodeWeightStats(20, 40, 60, 80)),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );
        // Move one shard and change the node weights.
        DesiredBalance thirdDesiredBalance = new DesiredBalance(
            1,
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of("a", "d"), 2, 0, 0)),
            Map.of(DUMMY_NODE, new DesiredBalanceMetrics.NodeWeightStats(30, 60, 90, 120)),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );

        var firstSummary = AllocationBalancingRoundSummaryService.createBalancerRoundSummary(firstDesiredBalance, secondDesiredBalance);
        var secondSummary = AllocationBalancingRoundSummaryService.createBalancerRoundSummary(secondDesiredBalance, thirdDesiredBalance);

        assertEquals(2, firstSummary.numberOfShardsToMove());
        assertEquals(1, firstSummary.nodeNameToWeightChanges().size());
        var firstSummaryWeights = firstSummary.nodeNameToWeightChanges().get(DUMMY_NODE.getName());
        assertEquals(10, firstSummaryWeights.baseWeights().shardCount());
        assertDoublesEqual(20, firstSummaryWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(30, firstSummaryWeights.baseWeights().writeLoad());
        assertDoublesEqual(40, firstSummaryWeights.baseWeights().nodeWeight());
        assertEquals(10, firstSummaryWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(20, firstSummaryWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(30, firstSummaryWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(40, firstSummaryWeights.weightsDiff().totalWeightDiff());

        assertEquals(1, secondSummary.numberOfShardsToMove());
        assertEquals(1, secondSummary.nodeNameToWeightChanges().size());
        var secondSummaryWeights = secondSummary.nodeNameToWeightChanges().get(DUMMY_NODE.getName());
        assertEquals(20, secondSummaryWeights.baseWeights().shardCount());
        assertDoublesEqual(40, secondSummaryWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(60, secondSummaryWeights.baseWeights().writeLoad());
        assertDoublesEqual(80, secondSummaryWeights.baseWeights().nodeWeight());
        assertEquals(10, secondSummaryWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(20, secondSummaryWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(30, secondSummaryWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(40, secondSummaryWeights.weightsDiff().totalWeightDiff());
    }

    /**
     * Tests that removing a node from old to new DesiredBalance will result in a weights diff of negative values bringing the weights down
     * to zero.
     */
    public void testCreateBalancerRoundSummaryWithRemovedNode() {
        DesiredBalance firstDesiredBalance = new DesiredBalance(
            1,
            // The shard assignments and node weights don't make sense together, but for summary purposes the first determines the summary's
            // number of shards moved, and the second the weight changes: the summary service doesn't need them to make sense together
            // because it looks at them separately. They do have to make sense individually across balancing rounds.
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of(DUMMY_NODE.getId()), 1, 0, 0)),
            Map.of(
                DUMMY_NODE,
                new DesiredBalanceMetrics.NodeWeightStats(10, 20, 30, 40),
                SECOND_DUMMY_NODE,
                new DesiredBalanceMetrics.NodeWeightStats(5, 15, 25, 35)
            ),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );
        // Remove a new node and don't move any shards.
        DesiredBalance secondDesiredBalance = new DesiredBalance(
            1,
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of(DUMMY_NODE.getId()), 1, 0, 0)),
            Map.of(DUMMY_NODE, new DesiredBalanceMetrics.NodeWeightStats(20, 40, 60, 80)),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );

        var summary = AllocationBalancingRoundSummaryService.createBalancerRoundSummary(firstDesiredBalance, secondDesiredBalance);

        assertEquals(0, summary.numberOfShardsToMove());
        assertEquals(2, summary.nodeNameToWeightChanges().size());

        var summaryDummyNodeWeights = summary.nodeNameToWeightChanges().get(DUMMY_NODE.getName());
        assertEquals(10, summaryDummyNodeWeights.baseWeights().shardCount());
        assertDoublesEqual(20, summaryDummyNodeWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(30, summaryDummyNodeWeights.baseWeights().writeLoad());
        assertDoublesEqual(40, summaryDummyNodeWeights.baseWeights().nodeWeight());
        assertEquals(10, summaryDummyNodeWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(20, summaryDummyNodeWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(30, summaryDummyNodeWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(40, summaryDummyNodeWeights.weightsDiff().totalWeightDiff());

        var summarySecondDummyNodeWeights = summary.nodeNameToWeightChanges().get(SECOND_DUMMY_NODE.getName());
        assertEquals(5, summarySecondDummyNodeWeights.baseWeights().shardCount());
        assertDoublesEqual(15, summarySecondDummyNodeWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(25, summarySecondDummyNodeWeights.baseWeights().writeLoad());
        assertDoublesEqual(35, summarySecondDummyNodeWeights.baseWeights().nodeWeight());
        assertEquals(-5, summarySecondDummyNodeWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(-15, summarySecondDummyNodeWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(-25, summarySecondDummyNodeWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(-35, summarySecondDummyNodeWeights.weightsDiff().totalWeightDiff());
    }

    /**
     * Tests that adding a node from old to new DesiredBalance will result in an entry in the summary for the new node with zero weights and
     * a weights diff showing the new allocation weight changes.
     */
    public void testCreateBalancerRoundSummaryWithAddedNode() {
        DesiredBalance firstDesiredBalance = new DesiredBalance(
            1,
            // The shard assignments and node weights don't make sense together, but for summary purposes the first determines the summary's
            // number of shards moved, and the second the weight changes: the summary service doesn't need them to make sense together
            // because it looks at them separately. They do have to make sense individually across balancing rounds.
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of(DUMMY_NODE.getId()), 1, 0, 0)),
            Map.of(DUMMY_NODE, new DesiredBalanceMetrics.NodeWeightStats(10, 20, 30, 40)),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );
        // Add a new node and move one shard.
        DesiredBalance secondDesiredBalance = new DesiredBalance(
            1,
            Map.of(new ShardId(INDEX_NAME, INDEX_UUID, 0), new ShardAssignment(Set.of(SECOND_DUMMY_NODE.getId()), 1, 0, 0)),
            Map.of(
                DUMMY_NODE,
                new DesiredBalanceMetrics.NodeWeightStats(20, 40, 60, 80),
                SECOND_DUMMY_NODE,
                new DesiredBalanceMetrics.NodeWeightStats(5, 15, 25, 35)
            ),
            DesiredBalance.ComputationFinishReason.CONVERGED
        );

        var summary = AllocationBalancingRoundSummaryService.createBalancerRoundSummary(firstDesiredBalance, secondDesiredBalance);

        assertEquals(1, summary.numberOfShardsToMove());
        assertEquals(2, summary.nodeNameToWeightChanges().size());

        var summaryDummyNodeWeights = summary.nodeNameToWeightChanges().get(DUMMY_NODE.getName());
        assertEquals(10, summaryDummyNodeWeights.baseWeights().shardCount());
        assertDoublesEqual(20, summaryDummyNodeWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(30, summaryDummyNodeWeights.baseWeights().writeLoad());
        assertDoublesEqual(40, summaryDummyNodeWeights.baseWeights().nodeWeight());
        assertEquals(10, summaryDummyNodeWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(20, summaryDummyNodeWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(30, summaryDummyNodeWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(40, summaryDummyNodeWeights.weightsDiff().totalWeightDiff());

        var summarySecondDummyNodeWeights = summary.nodeNameToWeightChanges().get(SECOND_DUMMY_NODE.getName());
        assertEquals(0, summarySecondDummyNodeWeights.baseWeights().shardCount());
        assertDoublesEqual(0, summarySecondDummyNodeWeights.baseWeights().diskUsageInBytes());
        assertDoublesEqual(0, summarySecondDummyNodeWeights.baseWeights().writeLoad());
        assertDoublesEqual(0, summarySecondDummyNodeWeights.baseWeights().nodeWeight());
        assertEquals(5, summarySecondDummyNodeWeights.weightsDiff().shardCountDiff());
        assertDoublesEqual(15, summarySecondDummyNodeWeights.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(25, summarySecondDummyNodeWeights.weightsDiff().writeLoadDiff());
        assertDoublesEqual(35, summarySecondDummyNodeWeights.weightsDiff().totalWeightDiff());
    }

    /**
     * Helper for double type inputs. assertEquals on double type inputs require a delta.
     */
    private void assertDoublesEqual(double expected, double actual) {
        assertEquals(expected, actual, 0.00001);
    }
}
