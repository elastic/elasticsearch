/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.node.usage;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools;
import org.elasticsearch.cluster.NodeUsageStatsForThreadPools.ThreadPoolUsageStats;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects some thread pool stats from each data node for purposes of shard allocation balancing. The specific stats are defined in
 * {@link NodeUsageStatsForThreadPools}.
 */
public class TransportNodeUsageStatsForThreadPoolsAction extends TransportNodesAction<
    NodeUsageStatsForThreadPoolsAction.Request,
    NodeUsageStatsForThreadPoolsAction.Response,
    NodeUsageStatsForThreadPoolsAction.NodeRequest,
    NodeUsageStatsForThreadPoolsAction.NodeResponse,
    Void> {

    public static final String NAME = "internal:monitor/thread_pool/stats";
    public static final ActionType<NodeUsageStatsForThreadPoolsAction.Response> TYPE = new ActionType<>(NAME);
    private static final int NO_VALUE = -1;

    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final AtomicLong lastMaxQueueLatencyMillis = new AtomicLong(NO_VALUE);

    @Inject
    public TransportNodeUsageStatsForThreadPoolsAction(
        ThreadPool threadPool,
        ClusterService clusterService,
        TransportService transportService,
        ActionFilters actionFilters,
        DesiredBalanceMetrics desiredBalanceMetrics
    ) {
        super(
            NAME,
            clusterService,
            transportService,
            actionFilters,
            NodeUsageStatsForThreadPoolsAction.NodeRequest::new,
            threadPool.executor(ThreadPool.Names.MANAGEMENT)
        );
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        desiredBalanceMetrics.registerWriteLoadDeciderMaxLatencyGauge(this::getMaxQueueLatencyMetric);
    }

    @Override
    protected NodeUsageStatsForThreadPoolsAction.Response newResponse(
        NodeUsageStatsForThreadPoolsAction.Request request,
        List<NodeUsageStatsForThreadPoolsAction.NodeResponse> nodeResponses,
        List<FailedNodeException> nodeFailures
    ) {

        return new NodeUsageStatsForThreadPoolsAction.Response(clusterService.getClusterName(), nodeResponses, nodeFailures);
    }

    @Override
    protected NodeUsageStatsForThreadPoolsAction.NodeRequest newNodeRequest(NodeUsageStatsForThreadPoolsAction.Request request) {
        return new NodeUsageStatsForThreadPoolsAction.NodeRequest();
    }

    @Override
    protected NodeUsageStatsForThreadPoolsAction.NodeResponse newNodeResponse(StreamInput in, DiscoveryNode node) throws IOException {
        return new NodeUsageStatsForThreadPoolsAction.NodeResponse(in);
    }

    @Override
    protected NodeUsageStatsForThreadPoolsAction.NodeResponse nodeOperation(
        NodeUsageStatsForThreadPoolsAction.NodeRequest request,
        Task task
    ) {
        DiscoveryNode localNode = clusterService.localNode();
        var writeExecutor = threadPool.executor(ThreadPool.Names.WRITE);
        assert writeExecutor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor;
        var trackingForWriteExecutor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) writeExecutor;

        long maxQueueLatencyMillis = Math.max(
            trackingForWriteExecutor.getMaxQueueLatencyMillisSinceLastPollAndReset(),
            trackingForWriteExecutor.peekMaxQueueLatencyInQueueMillis()
        );
        lastMaxQueueLatencyMillis.set(maxQueueLatencyMillis);
        ThreadPoolUsageStats threadPoolUsageStats = new ThreadPoolUsageStats(
            trackingForWriteExecutor.getMaximumPoolSize(),
            (float) trackingForWriteExecutor.pollUtilization(
                TaskExecutionTimeTrackingEsThreadPoolExecutor.UtilizationTrackingPurpose.ALLOCATION
            ),
            maxQueueLatencyMillis
        );

        return new NodeUsageStatsForThreadPoolsAction.NodeResponse(
            localNode,
            new NodeUsageStatsForThreadPools(localNode.getId(), Map.of(ThreadPool.Names.WRITE, threadPoolUsageStats))
        );
    }

    private Collection<LongWithAttributes> getMaxQueueLatencyMetric() {
        long maxQueueLatencyValue = lastMaxQueueLatencyMillis.getAndSet(NO_VALUE);
        if (maxQueueLatencyValue != NO_VALUE) {
            return Set.of(new LongWithAttributes(maxQueueLatencyValue));
        } else {
            return Set.of();
        }
    }
}
