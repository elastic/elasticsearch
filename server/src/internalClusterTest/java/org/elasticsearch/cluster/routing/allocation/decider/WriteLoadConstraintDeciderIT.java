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
import org.elasticsearch.action.admin.indices.stats.IndicesStatsTests;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.cache.query.QueryCacheStats;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.fielddata.FieldDataStats;
import org.elasticsearch.index.flush.FlushStats;
import org.elasticsearch.index.get.GetStats;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.search.stats.SearchStats;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.shard.SparseVectorStats;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.index.warmer.WarmerStats;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.suggest.completion.CompletionStats;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.elasticsearch.transport.TransportService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static java.util.Collections.emptyList;
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
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 100)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 100)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 100)
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), 100)
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final Collection<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        return plugins;
    }

    private DiscoveryNode getDiscoveryNode(String nodeName) {
        final TransportService transportService = internalCluster().getInstance(TransportService.class, nodeName);
        assertNotNull(transportService);
        return transportService.getLocalNode();
    }

    private static ShardStats getShardStats(IndexMetadata indexMeta, int shardIndex, double targetWriteLoad, String assignedShardNodeId) {
        ShardId shardId = new ShardId(indexMeta.getIndex(), shardIndex);
        Path path = createTempDir().resolve("indices").resolve(indexMeta.getIndexUUID()).resolve(String.valueOf(shardIndex));
        ShardRouting shardRouting = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        shardRouting = shardRouting.initialize(assignedShardNodeId, null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        CommonStats stats = new CommonStats();
        stats.docs = new DocsStats(100, 0, randomByteSizeValue().getBytes());
        stats.store = new StoreStats();
        stats.indexing = new IndexingStats(
            new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, targetWriteLoad)
        );
        return new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0);
    }

    public static IndicesStatsResponse randomIndicesStatsResponse(final IndexMetadata[] indices) {
        List<ShardStats> shardStats = new ArrayList<>();
        for (final IndexMetadata index : indices) {
            int numShards = index.getNumberOfShards();
            for (int i = 0; i < numShards; i++) {
                ShardId shardId = new ShardId(index.getIndex(), i);
                boolean primary = true;
                Path path = createTempDir().resolve("indices").resolve(index.getIndexUUID()).resolve(String.valueOf(i));
                ShardRouting shardRouting = ShardRouting.newUnassigned(
                    shardId,
                    primary,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
                    ShardRouting.Role.DEFAULT
                );
                shardRouting = shardRouting.initialize("node-0", null, ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                shardRouting = shardRouting.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
                CommonStats stats = new CommonStats();
                stats.fieldData = new FieldDataStats();
                stats.queryCache = new QueryCacheStats();
                stats.docs = new DocsStats();
                stats.store = new StoreStats();
                stats.indexing = new IndexingStats(
                    new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, 0.2)
                );
                stats.search = new SearchStats();
                stats.segments = new SegmentsStats();
                stats.merge = new MergeStats();
                stats.refresh = new RefreshStats();
                stats.completion = new CompletionStats();
                stats.requestCache = new RequestCacheStats();
                stats.get = new GetStats();
                stats.flush = new FlushStats();
                stats.warmer = new WarmerStats();
                stats.denseVectorStats = new DenseVectorStats();
                stats.sparseVectorStats = new SparseVectorStats();
                shardStats.add(new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0));
            }
        }
        return IndicesStatsTests.newIndicesStatsResponse(
            shardStats.toArray(new ShardStats[shardStats.size()]),
            shardStats.size(),
            shardStats.size(),
            0,
            emptyList(),
            ClusterState.EMPTY_STATE
        );
    }

    /**
     * Uses MockTransportService to set up write load stat responses from the data nodes and tests the allocation decisions made by the
     * balancer, specifically the effect of the {@link WriteLoadConstraintDecider}.
     *
     * Leverages the {@link FilterAllocationDecider} to first start all shards on NodeOne, and then eventually force the shards off of
     * NodeOne while NodeThree is hot-spotting, resulting in reassignment of all shards to NodeTwo.
     */
    public void testHighNodeWriteLoadPreventsNewShardAllocation() {
        final String masterName = internalCluster().startMasterOnlyNode();
        final var dataNodes = internalCluster().startDataOnlyNodes(3);
        final String firstDataNodeName = dataNodes.get(0);
        final String secondDataNodeName = dataNodes.get(1);
        final String thirdDataNodeName = dataNodes.get(2);
        final String firstDataNodeId = getNodeId(firstDataNodeName);
        final String secondDataNodeId = getNodeId(secondDataNodeName);
        final String thirdDataNodeId = getNodeId(thirdDataNodeName);
        ensureStableCluster(4);

        logger.info("---> first node name " + firstDataNodeName + " and ID " + firstDataNodeId + "; second node name "
            + secondDataNodeName + " and ID " + secondDataNodeId + "; third node name " + thirdDataNodeName + " and ID " + thirdDataNodeId);

        /**
         * Exclude assignment of shards to the second and third data nodes via the {@link FilterAllocationDecider} settings.
         * Then create an index with many shards, which will all be assigned to the first data node.
         */

        logger.info("---> Limit shard assignment to node " + firstDataNodeName + " by excluding the other nodes");
        updateClusterSettings(
            Settings.builder().put("cluster.routing.allocation.exclude._name", secondDataNodeName + "," + thirdDataNodeName)
        );

        String indexName = randomIdentifier();
        int randomNumberOfShards = randomIntBetween(15, 40); // Pick a high number of shards, so it is clear assignment is not accidental.

        var verifyAssignmentToFirstNodeListener = ClusterServiceUtils.addMasterTemporaryStateListener(
            clusterState -> {
                var indexRoutingTable = clusterState.routingTable().index(indexName);
                if (indexRoutingTable == null) {
                    return false;
                }
                return checkShardAssignment(
                    clusterState.getRoutingNodes(),
                    indexRoutingTable.getIndex(),
                    firstDataNodeId,
                    secondDataNodeId,
                    thirdDataNodeId,
                    randomNumberOfShards,
                    0,
                    0
                );
            }
        );

        createIndex(
            indexName,
            Settings.builder()
                .put(SETTING_NUMBER_OF_SHARDS, randomNumberOfShards)
                .put(SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        index(indexName, Integer.toString(randomInt(10)), Collections.singletonMap("foo", "bar"));
        ensureGreen(indexName);

        logger.info("---> Waiting for all shards to be assigned to node " + firstDataNodeName);
        safeAwait(verifyAssignmentToFirstNodeListener);

        /**
         * Override the {@link TransportNodeUsageStatsForThreadPoolsAction} and {@link TransportIndicesStatsAction} actions on the data
         * nodes to supply artificial write load stats. The stats will show the third node hot-spotting, and that all shards have non-empty
         * write load stats (so that the WriteLoadDecider will evaluate assigning them to a node).
         */

        final DiscoveryNode firstDiscoveryNode = getDiscoveryNode(firstDataNodeName);
        final DiscoveryNode secondDiscoveryNode = getDiscoveryNode(secondDataNodeName);
        final DiscoveryNode thirdDiscoveryNode = getDiscoveryNode(thirdDataNodeName);
        final NodeUsageStatsForThreadPools firstNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(firstDiscoveryNode, 2, 0.5f, 0);
        final NodeUsageStatsForThreadPools secondNodeNonHotSpottingNodeStats = createNodeUsageStatsForThreadPools(secondDiscoveryNode, 2, 0.5f, 0);
        final NodeUsageStatsForThreadPools thirdNodeHotSpottingNodeStats = createNodeUsageStatsForThreadPools(thirdDiscoveryNode, 2, 1.00f, 0);

        MockTransportService.getInstance(firstDataNodeName)
            .<NodeUsageStatsForThreadPoolsAction.NodeRequest>addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(firstDiscoveryNode, firstNodeNonHotSpottingNodeStats)
                )
            );
        MockTransportService.getInstance(secondDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) -> channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(secondDiscoveryNode, secondNodeNonHotSpottingNodeStats)
                )
            );
        MockTransportService.getInstance(thirdDataNodeName)
            .addRequestHandlingBehavior(
                TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
                (handler, request, channel, task) ->
                    channel.sendResponse(new NodeUsageStatsForThreadPoolsAction.NodeResponse(thirdDiscoveryNode, thirdNodeHotSpottingNodeStats))
            );
        // NOMERGE: put into addRequestHandlingBehavior
        ClusterState cs = internalCluster().getCurrentMasterNodeInstance(ClusterService.class).state();
        IndexMetadata indexMetadata = cs.getMetadata().getProject().index(indexName);

        double shardWriteLoadDefault = 0.2;
        MockTransportService.getInstance(firstDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                List<ShardStats> shardStats = new ArrayList<>(indexMetadata.getNumberOfShards());
                for (int i = 0; i < indexMetadata.getNumberOfShards(); i++) {
                    shardStats.add(
                        getShardStats(
                            indexMetadata,
                            i,
                            shardWriteLoadDefault,
                            firstDataNodeId
                        )
                    );
                }
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, firstDataNodeName);
                channel.sendResponse(instance.new NodeResponse(firstDataNodeId, indexMetadata.getNumberOfShards(), shardStats, List.of()));
            });
        MockTransportService.getInstance(secondDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                logger.info("~~~secondDataNodeName IndicesStatsAction");
                // Return no stats for the index because none are assigned to this node.
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, firstDataNodeName);
                channel.sendResponse(instance.new NodeResponse(secondDataNodeId, 0, List.of(), List.of()));
            });
        MockTransportService.getInstance(thirdDataNodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                logger.info("~~~thirdDataNodeName IndicesStatsAction");
                // Return no stats for the index because none are assigned to this node.
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, firstDataNodeName);
                channel.sendResponse(instance.new NodeResponse(thirdDataNodeId, 0, List.of(), List.of()));
            });

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
                clusterState -> {
                    Index index = clusterState.routingTable().index(indexName).getIndex();
                    return checkShardAssignment(
                        clusterState.getRoutingNodes(),
                        index,
                        firstDataNodeId,
                        secondDataNodeId,
                        thirdDataNodeId,
                        0,
                        randomNumberOfShards,
                        0
                    );
                }
            )
        );

    }

    /**
     * A handler to run the {@link TransportIndicesStatsAction} on the node as usual, but modifies the response to replace all shard write
     * load stats with the provided one instead, before returning the result to the waiting network channel. Essentially injects the desired
     * write load stat for all shards on the node.
     */
    public StubbableTransport.RequestHandlingBehavior<IndicesStatsRequest> runAndReplaceShardWriteLoadStats(double shardWriteLoadDefault) {
        return (handler, request, channel, task) -> {
            handler.messageReceived(
                request,
                new TestTransportChannel(
                    ActionListener.wrap(response -> {
                        var statsResponse = (IndicesStatsResponse) response;    //// NOMERGE: NOTE: need the same base response class?
                        ShardStats[] newShardStats = Arrays.stream(statsResponse.getShards()).map(shardStats -> {
                            CommonStats commonStats = new CommonStats();
                            commonStats.store = new StoreStats();
                            commonStats.indexing = new IndexingStats(
                                new IndexingStats.Stats(1, 1, 1, 1, 1, 1, 1, 1, 1, false, 1, 234, 234, 1000, 0.123, shardWriteLoadDefault)
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
                    }, e -> channel.sendResponse(e))
                ),
                task
            );
        };
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
