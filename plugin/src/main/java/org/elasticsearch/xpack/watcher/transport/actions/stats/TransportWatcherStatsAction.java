/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.WatcherService;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.transport.actions.WatcherTransportAction;
import org.elasticsearch.xpack.watcher.watch.Watch;

/**
 * Performs the stats operation.
 */
public class TransportWatcherStatsAction extends WatcherTransportAction<WatcherStatsRequest, WatcherStatsResponse> {

    private final WatcherService watcherService;
    private final ExecutionService executionService;
    private final WatcherLifeCycleService lifeCycleService;
    private final InternalClient client;

    @Inject
    public TransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                       ThreadPool threadPool, ActionFilters actionFilters,
                                       IndexNameExpressionResolver indexNameExpressionResolver, WatcherService watcherService,
                                       ExecutionService executionService, XPackLicenseState licenseState,
                                       WatcherLifeCycleService lifeCycleService, InternalClient client) {
        super(settings, WatcherStatsAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
                licenseState, WatcherStatsRequest::new);
        this.watcherService = watcherService;
        this.executionService = executionService;
        this.lifeCycleService = lifeCycleService;
        this.client = client;
    }

    @Override
    protected String executor() {
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
        statsResponse.setThreadPoolMaxSize(executionService.executionThreadPoolMaxSize());
        statsResponse.setWatcherMetaData(lifeCycleService.watcherMetaData());
        if (request.includeCurrentWatches()) {
            statsResponse.setSnapshots(executionService.currentExecutions());
        }
        if (request.includeQueuedWatches()) {
            statsResponse.setQueuedWatches(executionService.queuedWatches());
        }

        SearchRequest searchRequest =
                Requests.searchRequest(Watch.INDEX).types(Watch.DOC_TYPE).source(new SearchSourceBuilder().size(0));
        client.search(searchRequest, ActionListener.wrap(searchResponse -> {
                    statsResponse.setWatchesCount(searchResponse.getHits().getTotalHits());
                    listener.onResponse(statsResponse);
                },
                e -> listener.onResponse(statsResponse)));
    }

    @Override
    protected ClusterBlockException checkBlock(WatcherStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }
}
