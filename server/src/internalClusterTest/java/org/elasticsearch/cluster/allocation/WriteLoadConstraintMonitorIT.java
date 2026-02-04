/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class WriteLoadConstraintMonitorIT extends ESIntegTestCase {

    private static final Set<String> NOT_PREFERRED_NODES = Collections.newSetFromMap(new ConcurrentHashMap<>());

    private static boolean ALLOCATION_DISABLED = false;

    @After
    public void clearNotPreferredNodesAndAllocationDisabled() {
        NOT_PREFERRED_NODES.clear();
        ALLOCATION_DISABLED = false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockTransportService.TestPlugin.class);
        plugins.add(NotPreferredPlugin.class);
        return plugins;
    }

    @TestLogging(
        value = "org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor:TRACE",
        reason = "so we can see what the monitor is doing"
    )
    public void testRerouteIsCalledWhenHotSpotAppears() {
        // Set the threshold very high so we don't get any non-synthetic hot-spotting occurring
        final long queueLatencyThresholdMillis = randomLongBetween(50_000, 100_000);
        final Settings settings = enabledWriteLoadDeciderSettings(queueLatencyThresholdMillis);
        internalCluster().startMasterOnlyNode(settings);
        final String dataNodeOne = internalCluster().startDataOnlyNode(settings);
        final String dataNodeTwo = internalCluster().startDataOnlyNode(settings);
        // Maintain a third node so that there's always at least one non-hot-spotting node that can receive shards.
        internalCluster().startDataOnlyNode(settings);

        // Unmodified cluster info should detect no hot-spotting nodes
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "no hot-spots detected",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.TRACE,
                "No hot-spotting write nodes detected"
            )
        );

        // Simulate hot-spotting on a node
        simulateHotSpottingOnNode(dataNodeOne, queueLatencyThresholdMillis);

        // Single node hot-spotting should trigger reroute
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "hot spot detected message",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format("""
                    Nodes [[%s]] are hot-spotting, of 3 total ingest nodes. Reroute for hot-spotting has never previously been called. \
                    Previously hot-spotting nodes are [0 nodes]. The write thread pool queue latency threshold is [%s]. \
                    Triggering reroute.
                    """, getNodeId(dataNodeOne), TimeValue.timeValueMillis(queueLatencyThresholdMillis))
            )
        );

        // We should skip another re-route if no additional nodes are hot-spotting
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "reroute skipped due to being called recently",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format(
                    "Not calling reroute because we called reroute [*] ago and there are no new hot spots",
                    getNodeId(dataNodeOne),
                    TimeValue.timeValueMillis(queueLatencyThresholdMillis)
                )
            )
        );

        // Simulate hot-spotting on an additional node
        simulateHotSpottingOnNode(dataNodeTwo, queueLatencyThresholdMillis);

        // Additional node hot-spotting should trigger reroute
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "hot spot detected message",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.DEBUG,
                Strings.format("""
                    Nodes [[*]] are hot-spotting, of 3 total ingest nodes. \
                    Reroute for hot-spotting was last called [*] ago. Previously hot-spotting nodes are [[%s]]. \
                    The write thread pool queue latency threshold is [%s]. Triggering reroute.
                    """, getNodeId(dataNodeOne), TimeValue.timeValueMillis(queueLatencyThresholdMillis))
            )
        );

        // Clear simulated hot-spotting
        MockTransportService.getInstance(dataNodeOne).clearAllRules();
        MockTransportService.getInstance(dataNodeTwo).clearAllRules();

        // We should again detect no hot-spotting nodes
        MockLog.awaitLogger(
            ESIntegTestCase::refreshClusterInfo,
            WriteLoadConstraintMonitor.class,
            new MockLog.SeenEventExpectation(
                "no hot-spots detected",
                WriteLoadConstraintMonitor.class.getCanonicalName(),
                Level.TRACE,
                "No hot-spotting write nodes detected"
            )
        );
    }

    public void testAllocatorDoesNotMoveShardsToNotPreferredNode() {
        final Settings settings = Settings.builder()
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .put(BalancedShardsAllocator.DISK_USAGE_BALANCE_FACTOR_SETTING.getKey(), 0.0f)
            .build();

        internalCluster().startMasterOnlyNode(settings);

        /* set up a node that has a few indices, hotspot it against a not preferred node,
        and see that it doesn't move any shards onto it */
        final String sourceNode = internalCluster().startDataOnlyNode(settings);
        final String sourceNodeId = getNodeId(sourceNode);

        final int numberOfNodes = randomIntBetween(5, 20);
        final int numberOfIndices = numberOfNodes * 2;
        final int numberOfNotPreferredNodes = randomIntBetween(2, numberOfNodes - 2);

        Set<String> indexNames = new HashSet<>(numberOfIndices);
        for (int i = 0; i < numberOfIndices; i++) {
            final var indexName = randomIdentifier();
            indexNames.add(indexName);
            createIndex(indexName, 1, 1);
        }

        // check that the indices are on the only source hotspot node, beforehand
        ClusterStateResponse clusterState = safeGet(
            internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
        );
        for (String indexName : indexNames) {
            assertEquals(
                sourceNodeId,
                clusterState.getState().routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
            );
        }

        // turn off allocation, and add a bunch of nodes
        ALLOCATION_DISABLED = true;
        List<String> nodeNames = internalCluster().startDataOnlyNodes(numberOfNodes, settings);
        Set<String> nodeIds = new HashSet<>(nodeNames.stream().map(nodeName -> getNodeId(nodeName)).collect(Collectors.toSet()));
        Set<String> notPreferredNodeIds = new HashSet<>(randomSubsetOf(numberOfNotPreferredNodes, nodeIds));
        Set<String> preferredNodeIds = Sets.difference(nodeIds, notPreferredNodeIds);
        Set<String> preferredAndSourceNodeIDS = Sets.union(preferredNodeIds, Set.of(sourceNodeId));

        // check that all the shards are still assigned to the source node
        clusterState = safeGet(internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)));
        for (String indexName : indexNames) {
            assertEquals(
                sourceNodeId,
                clusterState.getState().routingTable(ProjectId.DEFAULT).index(indexName).shard(0).primaryShard().currentNodeId()
            );
        }

        // set up not preferred nodes, then turn back on allocation
        for (String nodeId : notPreferredNodeIds) {
            NOT_PREFERRED_NODES.add(nodeId);
        }
        ALLOCATION_DISABLED = false;

        // run reroute
        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );

        // wait for green on every index (replica has been distributed)
        for (String indexName : indexNames) {
            ensureGreen(indexName);
        }

        // check that all of the shards are on the source/preferred nodes
        clusterState = safeGet(internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)));
        var projectRoutingTable = clusterState.getState().routingTable(ProjectId.DEFAULT);
        int countOnPreferred = 0;
        for (String indexName : indexNames) {
            var shardRoutingTable = projectRoutingTable.index(indexName).shard(0);
            String primaryNodeId = shardRoutingTable.primaryShard().currentNodeId();
            String secondaryNodeId = shardRoutingTable.replicaShards().get(0).currentNodeId();
            assertTrue("Not preferred nodes should not have any shards", notPreferredNodeIds.contains(primaryNodeId) == false);
            assertTrue("Not preferred nodes should not have any shards", notPreferredNodeIds.contains(secondaryNodeId) == false);
            assertTrue("Preferred nodes or source should have all the shards", preferredAndSourceNodeIDS.contains(primaryNodeId));
            assertTrue("Preferred nodes or source should have all the shards", preferredAndSourceNodeIDS.contains(secondaryNodeId));
            if (preferredNodeIds.contains(primaryNodeId) || preferredNodeIds.contains(secondaryNodeId)) {
                countOnPreferred++;
            }
        }

        assertTrue("At least half of the shards should be moved off the source node", countOnPreferred > numberOfIndices / 2.0);
    }

    private void simulateHotSpottingOnNode(String nodeName, long queueLatencyThresholdMillis) {
        MockTransportService.getInstance(nodeName)
            .addRequestHandlingBehavior(TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]", (handler, request, channel, task) -> {
                handler.messageReceived(
                    request,
                    new TestTransportChannel(new ChannelActionListener<>(channel).delegateFailure((l, response) -> {
                        NodeUsageStatsForThreadPoolsAction.NodeResponse r = (NodeUsageStatsForThreadPoolsAction.NodeResponse) response;
                        l.onResponse(
                            new NodeUsageStatsForThreadPoolsAction.NodeResponse(
                                r.getNode(),
                                new NodeUsageStatsForThreadPools(
                                    r.getNodeUsageStatsForThreadPools().nodeId(),
                                    addQueueLatencyToWriteThreadPool(
                                        r.getNodeUsageStatsForThreadPools().threadPoolUsageStatsMap(),
                                        queueLatencyThresholdMillis
                                    )
                                )
                            )
                        );
                    })),
                    task
                );
            });
    }

    private Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> addQueueLatencyToWriteThreadPool(
        Map<String, NodeUsageStatsForThreadPools.ThreadPoolUsageStats> stringThreadPoolUsageStatsMap,
        long queueLatencyThresholdMillis
    ) {
        return stringThreadPoolUsageStatsMap.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> {
            NodeUsageStatsForThreadPools.ThreadPoolUsageStats originalStats = e.getValue();
            if (e.getKey().equals(ThreadPool.Names.WRITE)) {
                return new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                    originalStats.totalThreadPoolThreads(),
                    originalStats.averageThreadPoolUtilization(),
                    randomLongBetween(queueLatencyThresholdMillis * 2, queueLatencyThresholdMillis * 3)
                );
            }
            return originalStats;
        }));

    }

    /**
     * Enables the write-load decider and overrides other write load decider settings.
     * @param queueLatencyThresholdMillis Exceeding this is what makes the monitor call re-route
     */
    private Settings enabledWriteLoadDeciderSettings(long queueLatencyThresholdMillis) {
        return Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_QUEUE_LATENCY_THRESHOLD_SETTING.getKey(),
                TimeValue.timeValueMillis(queueLatencyThresholdMillis)
            )
            // Make the re-route interval large so we can test it
            .put(WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_REROUTE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMinutes(5))
            .build();
    }

    public static class NotPreferredPlugin extends Plugin implements ClusterPlugin {

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {

            return List.of(new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    Set<String> nodeIds = NOT_PREFERRED_NODES;
                    if (nodeIds.contains(node.nodeId())) {
                        return Decision.NOT_PREFERRED;
                    } else {
                        return Decision.YES;
                    }
                }
            }, new AllocationDecider() {
                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    if (ALLOCATION_DISABLED) {
                        return Decision.NO;
                    } else {
                        return Decision.YES;
                    }
                }
            });
        }
    }
}
