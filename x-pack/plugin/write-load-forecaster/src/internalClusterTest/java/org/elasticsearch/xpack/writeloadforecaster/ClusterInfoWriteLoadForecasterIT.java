/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.writeloadforecaster;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.stats.CommonStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.admin.indices.stats.TransportIndicesStatsAction;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.IndexBalanceConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoWriteLoadForecasterIT extends ESIntegTestCase {

    private final Map<ShardId, Double> shardWriteLoad = new HashMap<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            MockTransportService.TestPlugin.class,
            WriteLoadForecasterIT.FakeLicenseWriteLoadForecasterPlugin.class,
            RebalanceDisablerPlugin.class
        );
    }

    /**
     * Test that the ClusterInfoWriteLoadForecaster is able to correctly order shard placement in a cluster,
     * so that shard movements in the moveShards phase of the balancer works to move shards from the heaviest
     * node (in shard write load) to the lightest. Balancing is turned off.
     *
     * This test sets up a collection of nodes, then mocks out the IndexStats transport action to assign them
     * write loads in some ordering from light to heavy. Then, once the shards have been spread out so there
     * are three per node, it hotspots the heaviest node and checks that the shard is moved to the lightest node.
     */
    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
        reason = "so we can see the monitor hotspot message"
    )
    public void testWriteLoadDefinedRelocationOrdering() {
        final long queueLatencyThresholdMillis = randomLongBetween(3000, 7000);
        final int utilizationThresholdPercent = randomIntBetween(80, 99);
        final Settings settings = createClusterInfoWriteLoadForecasterTestSettings(
            queueLatencyThresholdMillis,
            utilizationThresholdPercent
        );
        internalCluster().startMasterOnlyNode(settings);

        // create a bunch of nodes, and three times the number of indices
        int numberOfNodes = randomIntBetween(3, 6);
        int numberOfIndices = 3 * numberOfNodes;

        List<String> nodeNames = internalCluster().startNodes(
            numberOfNodes,
            Settings.builder().put(settings).put(onlyRole(DiscoveryNodeRole.INDEX_ROLE)).build()
        );

        Set<String> indexNames = new HashSet<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final var indexName = randomIdentifier();
            indexNames.add(indexName);
            createIndex(indexName, 1, 0);
        }

        ClusterRerouteUtils.reroute(client());

        // assert that all nodes have three shards
        ClusterState state = clusterService().state();
        for (RoutingNode routingNode : state.getRoutingNodes()) {
            assertThat(routingNode.numberOfShardsWithState(ShardRoutingState.STARTED), equalTo(3));
        }

        // turn off shard balance factor (was needed to spread shards evenly)
        updateClusterSettings(Settings.builder().put(settings).put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.0f));

        // create map of node ids -> indices
        Map<String, List<IndexMetadata>> nodeIdsToIndexMetadata = new HashMap<>();
        RoutingTable routingTable = clusterService().state().routingTable(ProjectId.DEFAULT);
        for (String indexName : indexNames) {
            nodeIdsToIndexMetadata.computeIfAbsent(
                routingTable.index(indexName).shard(0).primaryShard().currentNodeId(),
                (String nodeId) -> new ArrayList<IndexMetadata>()
            ).add(clusterService().state().metadata().getProject().index(indexName));
        }

        // order shards with an inrease in shard write load, ensuring that the max shard write load +
        // the mininum will always be less than 0.9 for any node (allocation utilization thresholds
        // will block this)
        Collections.shuffle(nodeNames, random());
        double shardWriteLoadBase = 0.1;
        for (String nodeName : nodeNames) {
            List<ShardStats> shardStats = new ArrayList<>();
            for (IndexMetadata indexMetadata : nodeIdsToIndexMetadata.getOrDefault(getNodeId(nodeName), List.of())) {
                shardStats.add(
                    createShardStats(
                        indexMetadata,
                        0,
                        randomLongBetween(0L, 400_000_000L),
                        randomDoubleBetween(shardWriteLoadBase, shardWriteLoadBase + 0.1, true),
                        getNodeId(nodeName)
                    )
                );
            }
            mockShardStatsForNode(clusterService().state(), nodeName, shardStats);
            shardWriteLoadBase += .1;
        }

        // ensure that the write loads get propagated before the hotspot does
        refreshClusterInfo();

        // turn on cluster info write load forecaster, checking that this setting is interpreted live
        // and testing partly that the cluster info is passed into the delegate
        updateClusterSettings(
            Settings.builder()
                .put(settings)
                .put(WriteLoadConstraintSettings.CLUSTER_INFO_WRITE_LOAD_FORECASTER_ENABLED_SETTING.getKey(), true)
        );

        // simulate a hotspot on one of them, and see a shard move to the lowest one
        final String hotNode = nodeNames.get(nodeNames.size() - 1);
        final String hotNodeId = getNodeId(hotNode);
        final String coldNode = nodeNames.get(0);
        final String coldNodeId = getNodeId(coldNode);

        for (String nodeName : nodeNames) {
            simulateWriteLoadThreadPool(hotNode, queueLatencyThresholdMillis, utilizationThresholdPercent / 100.0f, nodeName == hotNode);
        }

        // check hotspot is detected
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "hot spot detected message",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format("""
                    Nodes [%s] are hot-spotting, of * total ingest nodes. Reroute for hot-spotting has never previously been called. \
                    Previously hot-spotting nodes are [0 nodes]. The write thread pool queue latency threshold is [*] and the \
                    utilization threshold is [*]. Triggering reroute.
                    """, hotNodeId + "/" + hotNode)
            )
        );

        // wait for the hotspot node to have fewer than its initial 3, and the lightest write load node to have more than 3
        awaitClusterState(clusterState -> {
            var routingNodes = clusterState.getRoutingNodes();
            int hotCount = routingNodes.node(hotNodeId).numberOfShardsWithState(ShardRoutingState.STARTED);
            int coldCount = routingNodes.node(coldNodeId).numberOfShardsWithState(ShardRoutingState.STARTED);
            return hotCount < 3 && coldCount > 3;
        });
    }

    public static ShardStats createShardStats(
        IndexMetadata indexMeta,
        int shardIndex,
        long shardDiskSize,
        double shardWriteLoad,
        String assignedShardNodeId
    ) {
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
        stats.store = new StoreStats(shardDiskSize, 1, 1);
        stats.indexing = new IndexingStats(
            new IndexingStats.Stats(
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                1,
                false,
                1,
                234,
                234,
                1000,
                shardWriteLoad,
                randomDoubleBetween(0.0, 20.0, true)
            )
        );
        return new ShardStats(shardRouting, new ShardPath(false, path, path, shardId), stats, null, null, null, false, 0);
    }

    public void mockShardStatsForNode(ClusterState clusterState, String nodeName, List<ShardStats> shards) {
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(IndicesStatsAction.NAME + "[n]", (handler, request, channel, task) -> {
                TransportIndicesStatsAction instance = internalCluster().getInstance(TransportIndicesStatsAction.class, nodeName);
                channel.sendResponse(instance.new NodeResponse(getNodeId(nodeName), shards.size(), shards, List.of()));
            });
    }

    private void simulateWriteLoadThreadPool(
        String nodeName,
        long queueLatencyThresholdMillis,
        float utilizationThreshold,
        boolean hotspot
    ) {
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]", (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(new ChannelActionListener<>(channel).delegateFailure((l, response) -> {
                        NodeUsageStatsForThreadPoolsAction.NodeResponse r = (NodeUsageStatsForThreadPoolsAction.NodeResponse) response;
                        Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> usageStats;
                        l.onResponse(
                            new NodeUsageStatsForThreadPoolsAction.NodeResponse(
                                r.getNode(),
                                new NodeUsageStatsForThreadPools(
                                    r.getNodeUsageStatsForThreadPools().nodeId(),
                                    simulateWriteThreadPool(
                                        r.getNodeUsageStatsForThreadPools().threadPoolUsageStatsMap(),
                                        queueLatencyThresholdMillis,
                                        utilizationThreshold,
                                        hotspot
                                    )
                                )
                            )
                        );
                    })),
                    task
                );
            });
    }

    private Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> simulateWriteThreadPool(
        Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> stringThreadPoolUsageStatsMap,
        long queueLatencyThresholdMillis,
        float utilizationThreshold,
        boolean hotspot
    ) {
        final float utilization;
        final long latency;
        if (hotspot) {
            utilization = randomFloatBetween(utilizationThreshold + .01f, 1.1f, true);
            latency = randomLongBetween(queueLatencyThresholdMillis + 1, queueLatencyThresholdMillis * 3);
        } else {
            utilization = randomFloatBetween(0.0f, utilizationThreshold - .01f, true);
            latency = randomLongBetween(0, queueLatencyThresholdMillis - 1);
        }
        return stringThreadPoolUsageStatsMap.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> {
            NodeUsageStatsForThreadPools.ThreadPoolUsageStats originalStats = e.getValue();
            if (e.getKey().equals(ThreadPool.Names.WRITE)) {
                return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(originalStats.totalThreadPoolThreads(), utilization, latency);
            }
            return originalStats;
        }));
    }

    public Settings createClusterInfoWriteLoadForecasterTestSettings(long queueLatencyThresholdMillis, int utilizationThresholdPercent) {
        final Settings settings = Settings.builder()
            // turn off all other balance settings and deciders (except shard balance, for now)
            .put(BalancedShardsAllocator.SHARD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(IndexBalanceConstraintSettings.INDEX_BALANCE_DECIDER_ENABLED_SETTING.getKey(), false)
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(),
                TimeValue.timeValueMillis(queueLatencyThresholdMillis)
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_HOTSPOT_UTILIZATION_THRESHOLD_SETTING.getKey(),
                utilizationThresholdPercent + "%"
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ALLOCATION_UTILIZATION_THRESHOLD_SETTING.getKey(),
                utilizationThresholdPercent + "%"
            )
            .put(BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 1.0f)
            .put(WriteLoadConstraintSettings.CLUSTER_INFO_WRITE_LOAD_FORECASTER_ENABLED_SETTING.getKey(), false)
            .put(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_SHARD_WRITE_LOAD_TYPE_SETTING.getKey(), "RECENT")
            .build();
        return settings;
    }

    public static class RebalanceDisablerPlugin extends Plugin implements ClusterPlugin {
        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new AllocationDecider() {
                @Override
                public Decision canRebalance(RoutingAllocation allocation) {
                    return Decision.NO;
                }
            });
        }
    }
}
