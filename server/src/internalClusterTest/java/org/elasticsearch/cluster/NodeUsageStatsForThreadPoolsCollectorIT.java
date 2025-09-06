/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TestTransportChannel;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class NodeUsageStatsForThreadPoolsCollectorIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // Need to enable write load decider to enable node usage stats collection
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testMostRecentValueIsUsedWhenNodeRequestFails() {
        final var dataNodeName = internalCluster().startDataOnlyNode();
        final var dataNodeClusterService = internalCluster().getInstance(ClusterService.class, dataNodeName);
        final var dataNodeTransportService = MockTransportService.getInstance(dataNodeName);
        final var threadPoolName = randomFrom(ThreadPool.Names.GENERIC, ThreadPool.Names.WRITE, ThreadPool.Names.SEARCH);

        // Intercept the node request and return some fake values
        final int totalThreadPoolThreads = randomIntBetween(2, 40);
        final float averageThreadPoolUtilization = randomFloatBetween(0.0f, 1.0f, true);
        final long maxThreadPoolQueueLatencyMillis = randomLongBetween(0, 1000);
        dataNodeTransportService.addRequestHandlingBehavior(
            TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
            (handler, request, channel, task) -> {
                NodeUsageStatsForThreadPoolsAction.NodeResponse response = safeAwait(
                    l -> handler.messageReceived(
                        request,
                        new TestTransportChannel(l.map(res -> (NodeUsageStatsForThreadPoolsAction.NodeResponse) res)),
                        task
                    )
                );
                final var responseStats = response.getNodeUsageStatsForThreadPools();
                channel.sendResponse(
                    new NodeUsageStatsForThreadPoolsAction.NodeResponse(
                        response.getNode(),
                        new NodeUsageStatsForThreadPools(
                            responseStats.nodeId(),
                            Maps.copyMapWithAddedOrReplacedEntry(
                                responseStats.threadPoolUsageStatsMap(),
                                threadPoolName,
                                new NodeUsageStatsForThreadPools.ThreadPoolUsageStats(
                                    totalThreadPoolThreads,
                                    averageThreadPoolUtilization,
                                    maxThreadPoolQueueLatencyMillis
                                )
                            ),
                            Instant.now()
                        )
                    )
                );
            }
        );

        // This info should contain our fake values
        final var successfulStats = assertThreadPoolHasStats(
            dataNodeClusterService.localNode().getId(),
            threadPoolName,
            totalThreadPoolThreads,
            averageThreadPoolUtilization,
            maxThreadPoolQueueLatencyMillis,
            null
        );

        // Now simulate an error
        dataNodeTransportService.addRequestHandlingBehavior(
            TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
            (handler, request, channel, task) -> {
                channel.sendResponse(new Exception("simulated error"));
            }
        );

        // The next response should also contain our fake values
        assertThreadPoolHasStats(
            dataNodeClusterService.localNode().getId(),
            threadPoolName,
            totalThreadPoolThreads,
            averageThreadPoolUtilization,
            maxThreadPoolQueueLatencyMillis,
            successfulStats.timestamp()
        );
    }

    private NodeUsageStatsForThreadPools assertThreadPoolHasStats(
        String nodeId,
        String threadPoolName,
        int totalThreadPoolThreads,
        float averageThreadPoolUtilization,
        long maxThreadPoolQueueLatencyMillis,
        @Nullable Instant timestamp
    ) {
        final var clusterInfo = Objects.requireNonNull(refreshClusterInfo());
        final var nodeUsageStatsForThreadPools = clusterInfo.getNodeUsageStatsForThreadPools().get(nodeId);
        if (timestamp != null) {
            assertThat(nodeUsageStatsForThreadPools.timestamp(), equalTo(timestamp));
        }
        final var usageStatsMap = nodeUsageStatsForThreadPools.threadPoolUsageStatsMap();
        assertThat(usageStatsMap, hasKey(threadPoolName));
        final var threadPoolStats = usageStatsMap.get(threadPoolName);
        assertThat(threadPoolStats.totalThreadPoolThreads(), equalTo(totalThreadPoolThreads));
        assertThat(threadPoolStats.averageThreadPoolUtilization(), equalTo(averageThreadPoolUtilization));
        assertThat(threadPoolStats.maxThreadPoolQueueLatencyMillis(), equalTo(maxThreadPoolQueueLatencyMillis));
        return nodeUsageStatsForThreadPools;
    }
}
