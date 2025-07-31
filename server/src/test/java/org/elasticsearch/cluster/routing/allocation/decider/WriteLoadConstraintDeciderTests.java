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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;

public class WriteLoadConstraintDeciderTests extends ESAllocationTestCase {

    // NOMERGE: I wonder if we don't want a node utilization percent, but rather a total node execution time used and a total node execution
    // time possible. The math would be a whole lot easier on us...
    public void testWriteLoadDeciderIsDisabled() {
        String indexName = "test-index";

        // Set up multiple nodes and an index with multiple shards.
        // DiscoveryNode discoveryNode1 = newNode("node1");
        // DiscoveryNode discoveryNode2 = newNode("node2");
        // ShardId shardId1 = new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 0);
        // ShardId shardId2 = new ShardId(indexName, IndexMetadata.INDEX_UUID_NA_VALUE, 1);

        /**
         * Create the ClusterState
         */

        ClusterState clusterState = ClusterStateCreationUtils.stateWithAssignedPrimariesAndReplicas(new String[] { indexName }, 3, 0);
        assertEquals(2, clusterState.nodes().size());
        assertEquals(1, clusterState.metadata().getTotalNumberOfIndices());

        /**
         * Fetch the nodes and index shards from the generated ClusterState.
         */

        var discoveryNodeIterator = clusterState.nodes().iterator();
        assertTrue(discoveryNodeIterator.hasNext());
        var exceedingThresholdDiscoveryNode = discoveryNodeIterator.next();
        assertTrue(discoveryNodeIterator.hasNext());
        var belowThresholdDiscoveryNode2 = discoveryNodeIterator.next();
        assertFalse(discoveryNodeIterator.hasNext());

        var indexIterator = clusterState.metadata().indicesAllProjects().iterator();
        assertTrue(indexIterator.hasNext());
        IndexMetadata testIndexMetadata = indexIterator.next();
        assertFalse(indexIterator.hasNext());
        Index testIndex = testIndexMetadata.getIndex();
        assertEquals(3, testIndexMetadata.getNumberOfShards());
        ShardId testShardId1 = new ShardId(testIndex, 0);
        ShardId testShardId2 = new ShardId(testIndex, 1);
        ShardId testShardId3NoWriteLoad = new ShardId(testIndex, 1);

        /**
         * Create the ClusterInfo that includes the node and shard level write load estimates.
         */

        var nodeThreadPoolStatsWithWriteExceedingThreshold = createNodeUsageStatsForThreadPools(
            exceedingThresholdDiscoveryNode,
            8,
            0.99f,
            0
        );
        var nodeThreadPoolStatsWithWriteBelowThreshold = createNodeUsageStatsForThreadPools(belowThresholdDiscoveryNode2, 8, 0.50f, 0);

        // Create a map of usage per node.
        var nodeIdToNodeUsageStatsForThreadPools = new HashMap<String, NodeUsageStatsForThreadPools>();
        nodeIdToNodeUsageStatsForThreadPools.put(exceedingThresholdDiscoveryNode.getId(), nodeThreadPoolStatsWithWriteExceedingThreshold);
        nodeIdToNodeUsageStatsForThreadPools.put(belowThresholdDiscoveryNode2.getId(), nodeThreadPoolStatsWithWriteBelowThreshold);

        var shardIdToWriteLoadEstimate = new HashMap<ShardId, Double>();
        shardIdToWriteLoadEstimate.put(testShardId1, 1.5);
        shardIdToWriteLoadEstimate.put(testShardId2, 1.5);
        shardIdToWriteLoadEstimate.put(testShardId3NoWriteLoad, 0d);

        ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeUsageStatsForThreadPools(nodeIdToNodeUsageStatsForThreadPools)
            .shardWriteLoads(shardIdToWriteLoadEstimate)
            .build();

        /**
         * Create the RoutingAllocation
         */

        var routingAllocation = new RoutingAllocation(
            null,
            RoutingNodes.immutable(clusterState.globalRoutingTable(), clusterState.nodes()),
            clusterState,
            clusterInfo,
            null,
            System.nanoTime()
        );

        ShardRouting shardRouting1 = TestShardRouting.newShardRouting(
            testShardId1,
            exceedingThresholdDiscoveryNode.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting shardRouting2 = TestShardRouting.newShardRouting(
            testShardId2,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );
        ShardRouting thirdRoutingNoWriteLoad = TestShardRouting.newShardRouting(
            testShardId3NoWriteLoad,
            belowThresholdDiscoveryNode2.getId(),
            null,
            true,
            ShardRoutingState.STARTED
        );

        assertTrue(discoveryNodeIterator.hasNext());
        RoutingNode exceedingThresholdRoutingNode = RoutingNodesHelper.routingNode(
            exceedingThresholdDiscoveryNode.getId(),
            discoveryNodeIterator.next(),
            shardRouting1
        );
        assertTrue(discoveryNodeIterator.hasNext());
        RoutingNode belowThresholdRoutingNode = RoutingNodesHelper.routingNode(
            belowThresholdDiscoveryNode2.getId(),
            discoveryNodeIterator.next(),
            shardRouting2
        );
        assertFalse(discoveryNodeIterator.hasNext());

        /**
         * Test the write load decider
         */

        // The write load decider is disabled by default.
        var writeLoadDecider = createWriteLoadConstraintDecider(Settings.builder().build());

        assertEquals(Decision.YES, writeLoadDecider.canAllocate(shardRouting2, exceedingThresholdRoutingNode, routingAllocation));
        assertEquals(Decision.YES, writeLoadDecider.canAllocate(shardRouting1, belowThresholdRoutingNode, routingAllocation));
        assertEquals(Decision.YES, writeLoadDecider.canAllocate(thirdRoutingNoWriteLoad, exceedingThresholdRoutingNode, routingAllocation));

        // Check that the answers change when enabled.
        writeLoadDecider = createWriteLoadConstraintDecider(
            Settings.builder()
                .put(
                    WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                    WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                )
                .build()
        );

        assertEquals(Decision.NO, writeLoadDecider.canAllocate(shardRouting2, exceedingThresholdRoutingNode, routingAllocation));
        assertEquals(Decision.YES, writeLoadDecider.canAllocate(shardRouting1, belowThresholdRoutingNode, routingAllocation));
        assertEquals(Decision.YES, writeLoadDecider.canAllocate(thirdRoutingNoWriteLoad, exceedingThresholdRoutingNode, routingAllocation));

        // NOMERGE: test that adding a shard is rejected if it would overflow the utilization threshold?
        // Need to implement the logic in the decider, I don't check right now.
    }

    public void testShardWithNoWriteLoadEstimateIsAlwaysYES() {
        Settings writeLoadConstraintSettings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .build();
        // TODO
    }

    public void testShardWithWriteLoadEstimate() {
        // TODO: test successful re-assignment and rejected re-assignment due to threshold
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
