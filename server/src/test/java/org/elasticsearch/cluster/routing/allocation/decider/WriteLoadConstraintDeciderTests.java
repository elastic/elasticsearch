/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;

public class WriteLoadConstraintDeciderTests extends ESAllocationTestCase {

    /**
     * Test the write load decider behavior when disabled
     */
    public void testWriteLoadDeciderDisabled() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.DISABLED
                )
                .build()
        );

        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.nearThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingNoWriteLoad,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );

        assertEquals(
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
    }

    /**
     * Test the {@link WriteLoadConstraintDecider#canAllocate} implementation.
     */
    public void testWriteLoadDeciderCanAllocate() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    randomBoolean()
                        ? WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                        : WriteLoadConstraintSettings.WriteLoadDeciderStatus.LOW_THRESHOLD_ONLY
                )
                .build()
        );
        assertDecisionMatches(
            "Assigning a new shard to a node that is above the threshold should fail",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "Node [*] with write thread pool utilization [0.99] already exceeds the high utilization threshold of [0.900000]. "
                + "Cannot allocate shard [[test-index][1]] to node without risking increased write latencies."
        );
        assertDecisionMatches(
            "Unassigned shard should always be accepted",
            writeLoadDecider.canAllocate(
                testHarness.unassignedShardRouting,
                randomFrom(testHarness.exceedingThresholdRoutingNode, testHarness.belowThresholdRoutingNode),
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            "Shard is unassigned. Decider takes no action."
        );
        assertDecisionMatches(
            "Assigning a new shard to a node that has capacity should succeed",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            null
        );
        assertDecisionMatches(
            "Assigning a new shard without a write load estimate should _not_ be blocked by lack of capacity",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingNoWriteLoad,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.YES,
            "Shard has no estimated write load. Decider takes no action."
        );
        assertDecisionMatches(
            "Assigning a new shard that would cause the node to exceed capacity should fail",
            writeLoadDecider.canAllocate(
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.nearThresholdRoutingNode,
                testHarness.routingAllocation
            ),
            Decision.Type.NOT_PREFERRED,
            "The high utilization threshold of [0.900000] would be exceeded on node [*] with utilization [0.89] "
                + "if shard [[test-index][0]] with estimated additional utilisation [0.06250] (write load [0.50000] / threads [8]) were "
                + "assigned to it. Cannot allocate shard to node without risking increased write latencies."
        );
    }

    /**
     * Test the {@link WriteLoadConstraintDecider#canRemain} implementation.
     */
    public void testWriteLoadDeciderCanRemain() {
        String indexName = "test-index";
        var testHarness = createClusterStateAndRoutingAllocation(indexName);

        var writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                )
                .build()
        );

        assertEquals(
            "A shard on a node below the util threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeBelowUtilThreshold,
                testHarness.belowThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node above the util threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeExceedingUtilThreshold,
                testHarness.exceedingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node with queuing below the threshold should remain on its node",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeBelowQueueThreshold,
                testHarness.belowQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard on a node with queuing above the threshold should not remain",
            Decision.Type.NOT_PREFERRED,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingOnNodeAboveQueueThreshold,
                testHarness.aboveQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
        assertEquals(
            "A shard without write load should remain on a node with queuing above the threshold",
            Decision.Type.YES,
            writeLoadDecider.canRemain(
                testHarness.clusterState.metadata().getProject().index(indexName),
                testHarness.shardRoutingNoWriteLoad,
                testHarness.aboveQueuingThresholdRoutingNode,
                testHarness.routingAllocation
            ).type()
        );
    }

    private void assertDecisionMatches(String description, Decision decision, Decision.Type type, String explanationPattern) {
        assertEquals(description, type, decision.type());
        if (explanationPattern == null) {
            assertNull(decision.getExplanation());
        } else {
            assertTrue(
                Strings.format("Expected: \"%s\", got \"%s\"", explanationPattern, decision.getExplanation()),
                Regex.simpleMatch(explanationPattern, decision.getExplanation())
            );
        }
    }

    /**
     * Carries all the cluster state objects needed for testing after {@link #createClusterStateAndRoutingAllocation} sets them up.
     */
    private record TestHarness(
        ClusterState clusterState,
        RoutingAllocation routingAllocation,
        RoutingNode exceedingThresholdRoutingNode,
        RoutingNode belowThresholdRoutingNode,
        RoutingNode nearThresholdRoutingNode,
        RoutingNode belowQueuingThresholdRoutingNode,
        RoutingNode aboveQueuingThresholdRoutingNode,
        ShardRouting shardRoutingOnNodeExceedingUtilThreshold,
        ShardRouting shardRoutingOnNodeBelowUtilThreshold,
        ShardRouting shardRoutingNoWriteLoad,
        ShardRouting shardRoutingOnNodeBelowQueueThreshold,
        ShardRouting shardRoutingOnNodeAboveQueueThreshold,
        ShardRouting unassignedShardRouting
    ) {}

    /**
     * Creates all the cluster state and objects needed to test the {@link WriteLoadConstraintDecider}.
     */
    private TestHarness createClusterStateAndRoutingAllocation(String indexName) {
        /**
         * Create the ClusterState for multiple nodes and multiple index shards.
         */

        int numberOfShards = 6;
        ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(
            new String[] { indexName },
            numberOfShards,
            3
        );
        // The number of data nodes the util method above creates is numberOfReplicas+2, and five data nodes are needed for this test.
        assertEquals(5, clusterState.nodes().size());
        assertEquals(1, clusterState.metadata().getTotalNumberOfIndices());

        /**
         * Fetch references to the nodes and index shards from the generated ClusterState, so the ClusterInfo can be created from them.
         */

        var discoveryNodeIterator = clusterState.nodes().iterator();
        assertTrue(discoveryNodeIterator.hasNext());
        var exceedingThresholdDiscoveryNode = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var belowThresholdDiscoveryNode2 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var nearThresholdDiscoveryNode3 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var queuingBelowThresholdDiscoveryNode4 = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var queuingAboveThresholdDiscoveryNode5 = discoveryNodeIterator.next();
        assertFalse(discoveryNodeIterator.hasNext());

        var indexIterator = clusterState.metadata().indicesAllProjects().iterator();
        assertTrue(indexIterator.hasNext());
        IndexMetadata testIndexMetadata = indexIterator.next();
        assertFalse(indexIterator.hasNext());
        Index testIndex = testIndexMetadata.getIndex();
        assertEquals(numberOfShards, testIndexMetadata.getNumberOfShards());
        ShardId testShardId1 = new ShardId(testIndex, 0);
        ShardId testShardId2 = new ShardId(testIndex, 1);
        ShardId testShardId3NoWriteLoad = new ShardId(testIndex, 2);
        ShardId testShardId4 = new ShardId(testIndex, 3);
        ShardId testShardId5 = new ShardId(testIndex, 4);
        ShardId testShardId6Unassigned = new ShardId(testIndex, 5);

        /**
         * Create a ClusterInfo that includes the node and shard level write load estimates for a variety of node capacity situations.
         */

        var nodeThreadPoolStatsWithWriteExceedingThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            0
        );
        var nodeThreadPoolStatsWithWriteBelowThreshold = createNodeUsageStatsForThreadPools(belowThresholdDiscoveryNode2, 8, 0.50f, 0);
        var nodeThreadPoolStatsWithWriteNearThreshold = createNodeUsageStatsForThreadPools(nearThresholdDiscoveryNode3, 8, 0.89f, 0);
        var nodeThreadPoolStatsWithQueuingBelowThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            5_000
        );
        var nodeThreadPoolStatsWithQueuingAboveThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            15_000
        );

        // Create a map of usage per node.
        var nodeIdToNodeUsageStatsForThreadPools = new HashMap<String, NodeUsageStatsForThreadPools>();
        nodeIdToNodeUsageStatsForThreadPools.put(exceedingThresholdDiscoveryNode.getId(), nodeThreadPoolStatsWithWriteExceedingThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(belowThresholdDiscoveryNode2.getId(), nodeThreadPoolStatsWithWriteBelowThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(nearThresholdDiscoveryNode3.getId(), nodeThreadPoolStatsWithWriteNearThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(queuingBelowThresholdDiscoveryNode4.getId(), nodeThreadPoolStatsWithQueuingBelowThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(queuingAboveThresholdDiscoveryNode5.getId(), nodeThreadPoolStatsWithQueuingAboveThreshold);

        // Create a map of usage per shard.
        var shardIdToWriteLoadEstimate = new HashMap<ShardId, Double>();
        shardIdToWriteLoadEstimate.put(testShardId1, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId2, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId3NoWriteLoad, 0d);
        shardIdToWriteLoadEstimate.put(testShardId4, 0.5);
        shardIdToWriteLoadEstimate.put(testShardId5, 0.5d);
        if (randomBoolean()) {
            shardIdToWriteLoadEstimate.put(testShardId6Unassigned, randomDoubleBetween(0.0, 2.0, true));
        }

        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(nodeIdToNodeUsageStatsForThreadPools)
            .shardWriteLoads(shardIdToWriteLoadEstimate)
            .build();

        /**
         * Create the RoutingAllocation from the ClusterState and ClusterInfo above, and set up the other input for the WriteLoadDecider.
         */

        var routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );

        ShardRouting shardRoutingOnNodeExceedingUtilThreshold = TestShardRouting.newShardRouting(
            testShardId1,
            exceedingThresholdDiscoveryNode.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeBelowUtilThreshold = TestShardRouting.newShardRouting(
            testShardId2,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingNoWriteLoad = TestShardRouting.newShardRouting(
            testShardId3NoWriteLoad,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeBelowQueueThreshold = TestShardRouting.newShardRouting(
            testShardId4,
            queuingBelowThresholdDiscoveryNode4.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRoutingOnNodeAboveQueueThreshold = TestShardRouting.newShardRouting(
            testShardId5,
            queuingAboveThresholdDiscoveryNode5.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting unassignedShardRouting = TestShardRouting.newShardRouting(
            testShardId6Unassigned,
            null,
            true,
            ShardRoutingState.UNASSIGNED
        );

        RoutingNode exceedingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            exceedingThresholdDiscoveryNode.getId(),
            exceedingThresholdDiscoveryNode,
            shardRoutingOnNodeExceedingUtilThreshold
        );
        RoutingNode belowThresholdRoutingNode = RoutingNodesHelper.routingNode(
            belowThresholdDiscoveryNode2.getId(),
            belowThresholdDiscoveryNode2,
            shardRoutingOnNodeBelowUtilThreshold
        );
        RoutingNode nearThresholdRoutingNode = RoutingNodesHelper.routingNode(
            nearThresholdDiscoveryNode3.getId(),
            nearThresholdDiscoveryNode3
        );
        RoutingNode belowQueuingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            queuingBelowThresholdDiscoveryNode4.getId(),
            queuingBelowThresholdDiscoveryNode4,
            shardRoutingOnNodeBelowQueueThreshold
        );
        RoutingNode aboveQueuingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            queuingAboveThresholdDiscoveryNode5.getId(),
            queuingAboveThresholdDiscoveryNode5,
            shardRoutingOnNodeAboveQueueThreshold
        );

        return new TestHarness(
            clusterState,
            routingAllocation,
            exceedingThresholdRoutingNode,
            belowThresholdRoutingNode,
            nearThresholdRoutingNode,
            belowQueuingThresholdRoutingNode,
            aboveQueuingThresholdRoutingNode,
            shardRoutingOnNodeExceedingUtilThreshold,
            shardRoutingOnNodeBelowUtilThreshold,
            shardRoutingNoWriteLoad,
            shardRoutingOnNodeBelowQueueThreshold,
            shardRoutingOnNodeAboveQueueThreshold,
            unassignedShardRouting
        );
    }

    private WriteLoadConstraintDecider createWriteLoadConstraintDecider(Settings settings) {
        return new WriteLoadConstraintDecider(createBuiltInClusterSettings(settings));
    }

    /**
     * Helper to create a {@link NodeUsageStatsForThreadPools} for the given node with the given WRITE thread pool usage stats.
     */
    private NodeUsageStatsForThreadPools createNodeUsageStatsForThreadPools(
        DiscoveryNode discoveryNode,
        int totalWriteThreadPoolThreads,
        float averageWriteThreadPoolUtilization,
        long averageWriteThreadPoolQueueLatencyMillis
    ) {

        // Create thread pool usage stats map for node1.
        var writeThreadPoolUsageStats = new ThreadPoolUsageStats(
            totalWriteThreadPoolThreads,
            averageWriteThreadPoolUtilization,
            averageWriteThreadPoolQueueLatencyMillis
        );
        var threadPoolUsageMap = new HashMap<String, ThreadPoolUsageStats>();
        threadPoolUsageMap.put(ThreadPool.Names.WRITE, writeThreadPoolUsageStats);

        // Create the node's thread pool usage map
        return new NodeUsageStatsForThreadPools(discoveryNode.getId(), threadPoolUsageMap);
    }
}
