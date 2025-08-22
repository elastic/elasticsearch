/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class WriteLoadConstraintDeciderIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .build();
    }

    /**
     * Uses MockTransportService to set up write load stat responses from the data nodes and tests the allocation decisions made by the
     * balancer, specifically the effect of the {@link WriteLoadConstraintDecider}.
     *
     * Leverages the {@link FilterAllocationDecider} to start all shards on Nod
     *
     * Can I override the stats results for the data nodes, trick the master into having higher stats than reality?
     * Set high shard move concurrency settings, and watch the cluster state updates for outcomes.
     *
     * Can I set the balancer write loads for the shards to some high even number, and then have the write load decider block rebalancing?
     *
     * Ultimately, unassigned shards should override the Decider.
     *
     * How can I force even shard assignment? And then force a reassignment attempt?
     * If I have 3 nodes, 2 shards, could I force shard movement away from NodeA with a filter, and then ensure re-assignment
     * to nodeB because nodeC is hot?
     * Let's make it a lot of index shards, and then they all get reassigned to nodeB.
     * Also, set a filter to initially get all the shards assigned to nodeA -- nice!
     * TODO: I need to set shard level write load else the Decider won't act...
     *
     */
    public void testHighNodeWriteLoadPreventsNewShardAllocation() {
        final var dataNodes = internalCluster().startDataOnlyNodes(3);
        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        final String masterName = internalCluster().startMasterOnlyNode();
        ensureStableCluster(3);

        /**
         * Exclude assignment of shards to the second and third data nodes via the {@link FilterAllocationDecider} settings.
         * Then create an index with many shards, which will all be assigned to the first data node.
         */

        String indexName = randomIdentifier();
        String indexUUID = randomAlphaOfLength(10);
        Index index = new Index(indexName, indexUUID);

        logger.info("---> Limit shard assignment to node " + firstDataNodeName + " by excluding the other nodes");
        updateClusterSettings(
            Settings.builder().put("cluster.routing.allocation.exclude._name", secondDataNodeName + "," + thirdDataNodeName)
        );

        int randomNumberOfShards = randomIntBetween(15, 40); // Pick a high number of shards, so it is clear assignment is not accidental.
        createIndex(
            indexName,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_INDEX_UUID, indexUUID)
                .build()
        );
        index(indexName, Integer.toString(randomInt(10)), Collections.singletonMap("foo", "bar"));
        ensureGreen(indexName);

        logger.info("---> Verifying that all shards are assigned to node " + firstDataNodeName);
        final RoutingNodes routingNodes = internalCluster().getInstance(RoutingNodes.class, masterName);
        assertTrue(
            Strings.format(
                "Expected all [%d] shards for index [%s] to be on node [%s]: [%s]",
                randomNumberOfShards,
                indexName,
                firstDataNodeName,
                routingNodes
            ),
            checkShardAssignment(routingNodes, index, firstDataNodeId, secondDataNodeId, thirdDataNodeId, randomNumberOfShards, 0, 0)
        );

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} and {@link TransportIndicesStatsAction} actions on the data
         * nodes to supply artificial write load stats. The stats will show the third node hot-spotting, and that all shards have non-empty
         * write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        final DiscoveryNode firstDiscoveryNode = internalCluster().getInstance(DiscoveryNode.class, firstDataNodeName);
        final DiscoveryNode secondDiscoveryNode = internalCluster().getInstance(DiscoveryNode.class, firstDataNodeName);
        final DiscoveryNode thirdDiscoveryNode = internalCluster().getInstance(DiscoveryNode.class, thirdDataNodeName);
        final NodeUsageStatsForThreadPools nonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(firstDiscoveryNode, 2, 0.5f, 0);
        final NodeUsageStatsForThreadPools hotSpottingNodeStats = createNodeUsageStatsForThreadPools(secondDiscoveryNode, 2, 1.00f, 0);

        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(firstDiscoveryNode, nonHotSpottingNodeStats)
                )
            );
        MockTransportService.getInstance(secondDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(secondDiscoveryNode, nonHotSpottingNodeStats)
                )
            );
        MockTransportService.getInstance(thirdDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(thirdDiscoveryNode, hotSpottingNodeStats)
                )
            );
        double shardWriteLoadDefault = 0.2;
        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", runAndReplaceShardWriteLoadStats(shardWriteLoadDefault));
        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", runAndReplaceShardWriteLoadStats(shardWriteLoadDefault));
        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", runAndReplaceShardWriteLoadStats(shardWriteLoadDefault));

        /**
         * Provoke a ClusterInfo stats refresh, update the cluster settings to shuffle shards off of the first node, and initiate
         * rebalancing via a reroute request. Then wait to see a cluster state update that reassigns all the shards away from the first
         * node as expected.
         */

        logger.info("---> Refreshing the cluster info to pull in the dummy thread pool stats with a hot-spotting node");
        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster().getInstance(
            ClusterInfoService.class,
            masterName
        );
        ClusterInfoServiceUtils.refresh(clusterInfoService);

        logger.info(
            "---> Update the filter to exclude " + firstDataNodeName + " so that shards will be reassigned away to the other nodes"
        );
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", firstDataNodeName));

        // Start a reroute to pick up the hot-spotting ClusterInfo and act upon it with the new filter and write load stats.
        ClusterRerouteUtils.reroute(client());

        safeAwait(
            ClusterServiceUtils.addMasterTemporaryStateListener(
                clusterState -> checkShardAssignment(
                    clusterState.getRoutingNodes(),
                    index,
                    firstDataNodeId,
                    secondDataNodeId,
                    thirdDataNodeId,
                    0,
                    randomNumberOfShards,
                    0
                )
            )
        );

    }

    /**
     * A handler to run the {@link TransportIndicesStatsAction} on the node as usual, but modifies the response to replace all shard write
     * load stats with the provided one instead, before returning the result to the waiting network channel. Essentially injects the desired
     * write load stat for all shards on the node.
     */
    public StubbableTransport.RequestHandlingBehavior<IndicesStatsRequest> runAndReplaceShardWriteLoadStats(double shardWriteLoadEstimate) {
        return (handler, request, channel, task) -> ActionListener.wrap(response -> {
            var statsResponse = (IndicesStatsResponse) response;
            ShardStats[] newShardStats = Arrays.stream(statsResponse.getShards()).map(shardStats -> {
                CommonStats commonStats = new CommonStats();
                commonStats.store = new StoreStats();
                commonStats.indexing = new IndexingStats(
                    new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, shardWriteLoadEstimate)
                );
                return new ShardStats(
                    shardStats.getShardRouting(),
                    commonStats,
                    shardStats.getCommitStats(),
                    shardStats.getSeqNoStats(),
                    shardStats.getRetentionLeaseStats(),
                    shardStats.getDataPath(),
                    shardStats.getStatePath(),
                    shardStats.isCustomDataPath(),
                    shardStats.isSearchIdle(),
                    shardStats.getSearchIdleTime()
                );
            }).toArray(ShardStats[]::new);
            channel.sendResponse(
                new IndicesStatsResponse(
                    newShardStats,
                    statsResponse.getTotalShards(),
                    statsResponse.getSuccessfulShards(),
                    statsResponse.getFailedShards(),
                    null,
                    Metadata.EMPTY_METADATA,
                    RoutingTable.EMPTY_ROUTING_TABLE
                )
            );
        }, e -> channel.sendResponse(e));
    }

    private boolean checkShardAssignment(
        RoutingNodes routingNodes,
        Index index,
        String firstDataNodeId,
        String secondDataNodeId,
        String thirdDataNodeId,
        int firstDataNodeExpectedNumShards,
        int secondDataNodeExpectedNumShards,
        int thirdDataNodeExpectedNumShards
    ) {

        int firstDataNodeRealNumberOfShards = routingNodes.node(firstDataNodeId).numberOfOwningShardsForIndex(index);
        if (firstDataNodeRealNumberOfShards != firstDataNodeExpectedNumShards) {
            return false;
        }
        int secondDataNodeRealNumberOfShards = routingNodes.node(secondDataNodeId).numberOfOwningShardsForIndex(index);
        if (secondDataNodeRealNumberOfShards != secondDataNodeExpectedNumShards) {
            return false;
        }
        int thirdDataNodeRealNumberOfShards = routingNodes.node(thirdDataNodeId).numberOfOwningShardsForIndex(index);
        if (thirdDataNodeRealNumberOfShards != thirdDataNodeExpectedNumShards) {
            return false;
        }

        return true;
    }

    // TODO: integration testing to see that the components work together.

    // NOMERGE: dedupe this with WriteLoadConstraintDeciderTests
    private NodeUsageStatsForThreadPools createNodeUsageStatsForThreadPools(
        DiscoveryNode discoveryNode,
        int totalWriteThreadPoolThreads,
        float averageWriteThreadPoolUtilization,
        long averageWriteThreadPoolQueueLatencyMillis
    ) {

        // Create thread pool usage stats map for node1.
        var writeThreadPoolUsageStats = new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
            totalWriteThreadPoolThreads,
            averageWriteThreadPoolUtilization,
            averageWriteThreadPoolQueueLatencyMillis
        );
        var threadPoolUsageMap = new HashMap<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats>();
        threadPoolUsageMap.put(ThreadPool.Names.WRITE, writeThreadPoolUsageStats);

        // Create the node's thread pool usage map
        return new NodeUsageStatsForThreadPools(discoveryNode.getId(), threadPoolUsageMap);
    }
}
