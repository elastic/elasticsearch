/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherBuild;
import org.elasticsearch.watcher.WatcherLifeCycleService;
import org.elasticsearch.watcher.WatcherService;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.WatcherLicensee;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;

/**
 * Performs the stats operation.
 */
public class TransportWatcherStatsAction extends WatcherTransportAction<WatcherStatsRequest, WatcherStatsResponse> {

    private final WatcherService watcherService;
    private final ExecutionService executionService;
    private final WatcherLifeCycleService lifeCycleService;

    @Inject
    public TransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                       ExecutionService executionService, WatcherLicensee watcherLicensee,
                                       WatcherLifeCycleService lifeCycleService) {
        super(settings, WatcherStatsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                watcherLicensee, WatcherStatsRequest::new);
        this.watcherService = watcherService;
        this.executionService = executionService;
        this.lifeCycleService = lifeCycleService;
    }

    @Override
    protected String executor() {
        // cheap operation, no need to fork into another thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected WatcherStatsResponse newResponse() {
        return new WatcherStatsResponse();
    }

    @Override
    protected void masterOperation(WatcherStatsRequest request, ClusterState state, ActionListener<WatcherStatsResponse> listener) throws
            ElasticsearchException {
        WatcherStatsResponse statsResponse = new WatcherStatsResponse();
        statsResponse.setWatcherState(watcherService.state());
        statsResponse.setThreadPoolQueueSize(executionService.executionThreadPoolQueueSize());
        statsResponse.setWatchesCount(watcherService.watchesCount());
        statsResponse.setThreadPoolMaxSize(executionService.executionThreadPoolMaxSize());
        statsResponse.setBuild(WatcherBuild.CURRENT);
        statsResponse.setWatcherMetaData(lifeCycleService.watcherMetaData());

        if (request.includeCurrentWatches()) {
            statsResponse.setSnapshots(executionService.currentExecutions());
        }
        if (request.includeQueuedWatches()) {
            statsResponse.setQueuedWatches(executionService.queuedWatches());
        }

        listener.onResponse(statsResponse);
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }


}
