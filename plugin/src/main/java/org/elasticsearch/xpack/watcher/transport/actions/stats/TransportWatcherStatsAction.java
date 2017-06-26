/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.nodes.TransportNodesAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;

import java.util.List;

/**
 * Performs the stats operation.
 */
public class TransportWatcherStatsAction extends TransportNodesAction<WatcherStatsRequest, WatcherStatsResponse,
        WatcherStatsRequest.Node, WatcherStatsResponse.Node> {

    private final WatcherService watcherService;
    private final ExecutionService executionService;
    private final TriggerService triggerService;
    private final WatcherLifeCycleService lifeCycleService;

    @Inject
    public TransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                       ExecutionService executionService, TriggerService triggerService,
                                       WatcherLifeCycleService lifeCycleService) {
        super(settings, WatcherStatsAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                WatcherStatsRequest::new, WatcherStatsRequest.Node::new, ThreadPool.Names.MANAGEMENT,
                WatcherStatsResponse.Node.class);
        this.watcherService = watcherService;
        this.executionService = executionService;
        this.triggerService = triggerService;
        this.lifeCycleService = lifeCycleService;
    }

    @Override
    protected WatcherStatsResponse newResponse(WatcherStatsRequest request, List<WatcherStatsResponse.Node> nodes,
                                               List<FailedNodeException> failures) {
        return new WatcherStatsResponse(clusterService.getClusterName(), lifeCycleService.watcherMetaData(), nodes, failures);
    }

    @Override
    protected WatcherStatsRequest.Node newNodeRequest(String nodeId, WatcherStatsRequest request) {
        return new WatcherStatsRequest.Node(request, nodeId);
    }

    @Override
    protected WatcherStatsResponse.Node newNodeResponse() {
        return new WatcherStatsResponse.Node();
    }

    @Override
    protected WatcherStatsResponse.Node nodeOperation(WatcherStatsRequest.Node request) {
        WatcherStatsResponse.Node statsResponse = new WatcherStatsResponse.Node(clusterService.localNode());
        statsResponse.setWatcherState(watcherService.state());
        statsResponse.setThreadPoolQueueSize(executionService.executionThreadPoolQueueSize());
        statsResponse.setThreadPoolMaxSize(executionService.executionThreadPoolMaxSize());
        if (request.includeCurrentWatches()) {
            statsResponse.setSnapshots(executionService.currentExecutions());
        }
        if (request.includeQueuedWatches()) {
            statsResponse.setQueuedWatches(executionService.queuedWatches());
        }

        statsResponse.setWatchesCount(triggerService.count());
        return statsResponse;
    }

}
