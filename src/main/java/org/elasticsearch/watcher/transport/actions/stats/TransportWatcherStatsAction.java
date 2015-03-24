/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.watcher.WatcherBuild;
import org.elasticsearch.watcher.watch.WatchService;
import org.elasticsearch.watcher.WatcherVersion;
import org.elasticsearch.watcher.history.HistoryService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the stats operation.
 */
public class TransportWatcherStatsAction extends TransportMasterNodeOperationAction<WatcherStatsRequest, WatcherStatsResponse> {

    private final WatchService watchService;
    private final HistoryService historyService;

    @Inject
    public TransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters, WatchService watchService,
                                       HistoryService historyService) {
        super(settings, WatcherStatsAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.watchService = watchService;
        this.historyService = historyService;
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
        statsResponse.setWatchServiceState(watchService.state());
        statsResponse.setWatchExecutionQueueSize(historyService.queueSize());
        statsResponse.setWatchesCount(watchService.watchesCount());
        statsResponse.setWatchExecutionQueueMaxSize(historyService.largestQueueSize());
        statsResponse.setVersion(WatcherVersion.CURRENT);
        statsResponse.setBuild(WatcherBuild.CURRENT);
        listener.onResponse(statsResponse);
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }


}
