/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.LicenseUtils;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.watcher.WatcherMetaData;
import org.elasticsearch.xpack.watcher.WatcherLifeCycleService;
import org.elasticsearch.xpack.watcher.execution.ExecutionService;
import org.elasticsearch.xpack.watcher.trigger.TriggerService;

/**
 * Performs the stats operation required for the rolling upfrade from 5.x
 */
public class OldTransportWatcherStatsAction extends TransportMasterNodeAction<OldWatcherStatsRequest, OldWatcherStatsResponse> {

    private final WatcherLifeCycleService watcherLifeCycleService;
    private final ExecutionService executionService;
    private final XPackLicenseState licenseState;
    private final TriggerService triggerService;

    @Inject
    public OldTransportWatcherStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                          ThreadPool threadPool, ActionFilters actionFilters,
                                          IndexNameExpressionResolver indexNameExpressionResolver,
                                          WatcherLifeCycleService watcherLifeCycleService, ExecutionService executionService,
                                          XPackLicenseState licenseState, TriggerService triggerService) {
        super(settings, OldWatcherStatsAction.NAME, transportService, clusterService, threadPool, actionFilters,
                indexNameExpressionResolver, OldWatcherStatsRequest::new);
        this.watcherLifeCycleService = watcherLifeCycleService;
        this.executionService = executionService;
        this.licenseState = licenseState;
        this.triggerService = triggerService;
    }

    @Override
    protected String executor() {
        // cheap operation, no need to fork into another thread
        return ThreadPool.Names.SAME;
    }

    @Override
    protected void doExecute(Task task, OldWatcherStatsRequest request, ActionListener<OldWatcherStatsResponse> listener) {
        if (licenseState.isWatcherAllowed()) {
            super.doExecute(task, request, listener);
        } else {
            listener.onFailure(LicenseUtils.newComplianceException(XPackField.WATCHER));
        }
    }

    @Override
    protected OldWatcherStatsResponse newResponse() {
        return new OldWatcherStatsResponse();
    }

    @Override
    protected void masterOperation(OldWatcherStatsRequest request, ClusterState state,
                                   ActionListener<OldWatcherStatsResponse> listener) throws ElasticsearchException {
        OldWatcherStatsResponse statsResponse = new OldWatcherStatsResponse();
        statsResponse.setWatcherState(watcherLifeCycleService.getState());
        statsResponse.setThreadPoolQueueSize(executionService.executionThreadPoolQueueSize());
        statsResponse.setWatchesCount(triggerService.count());
        statsResponse.setThreadPoolMaxSize(executionService.executionThreadPoolMaxSize());
        statsResponse.setWatcherMetaData(getWatcherMetaData());

        if (request.includeCurrentWatches()) {
            statsResponse.setSnapshots(executionService.currentExecutions());
        }
        if (request.includeQueuedWatches()) {
            statsResponse.setQueuedWatches(executionService.queuedWatches());
        }

        listener.onResponse(statsResponse);
    }

    @Override
    protected ClusterBlockException checkBlock(OldWatcherStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_READ);
    }

    private WatcherMetaData getWatcherMetaData() {
        WatcherMetaData watcherMetaData = clusterService.state().getMetaData().custom(WatcherMetaData.TYPE);
        if (watcherMetaData == null) {
            watcherMetaData = new WatcherMetaData(false);
        }
        return watcherMetaData;
    }
}
