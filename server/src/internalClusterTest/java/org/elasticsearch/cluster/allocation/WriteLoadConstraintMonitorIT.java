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
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintMonitor;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

@ESIntegTestCase.ClusterScope(numDataNodes = 0)
public class WriteLoadConstraintMonitorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
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
}
