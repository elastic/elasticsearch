/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.watcher.WatcherBuild;
import org.elasticsearch.watcher.WatcherVersion;
import org.elasticsearch.watcher.execution.ExecutionService;
import org.elasticsearch.watcher.license.LicenseService;
import org.elasticsearch.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.watcher.WatcherService;

/**
 * Performs the stats operation.
 */
public class TransportWatcherStatsAction extends WatcherTransportAction<WatcherStatsRequest, WatcherStatsResponse> {

    private final WatcherService watcherService;
    private final ExecutionService executionService;

    @Inject
    public TransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, WatcherService watcherService,
                                       ExecutionService executionService, LicenseService licenseService) {
        super(settings, WatcherStatsAction.NAME, transportService, clusterService, threadPool, actionFilters, licenseService);
        this.watcherService = watcherService;
        this.executionService = executionService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected WatcherStatsRequest newRequest() {
        return new WatcherStatsRequest();
    }

    @Override
    protected WatcherStatsResponse newResponse() {
        return new WatcherStatsResponse();
    }

    @Override
    protected void masterOperation(WatcherStatsRequest request, ClusterState state, ActionListener<WatcherStatsResponse> listener) throws ElasticsearchException {
        WatcherStatsResponse statsResponse = new WatcherStatsResponse();
        statsResponse.setWatchServiceState(watcherService.state());
        statsResponse.setWatchExecutionQueueSize(executionService.queueSize());
        statsResponse.setWatchesCount(watcherService.watchesCount());
        statsResponse.setWatchExecutionQueueMaxSize(executionService.largestQueueSize());
        statsResponse.setVersion(WatcherVersion.CURRENT);
        statsResponse.setBuild(WatcherBuild.CURRENT);
        listener.onResponse(statsResponse);
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }


}
