/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.AlertsBuild;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.AlertsVersion;
import org.elasticsearch.alerts.history.HistoryService;
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
public class TransportAlertsStatsAction extends TransportMasterNodeOperationAction<AlertsStatsRequest, AlertsStatsResponse> {

    private final AlertsService alertsService;
    private final HistoryService historyService;

    @Inject
    public TransportAlertsStatsAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters, AlertsService alertsService,
                                      HistoryService historyService) {
        super(settings, AlertsStatsAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.alertsService = alertsService;
        this.historyService = historyService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AlertsStatsRequest newRequest() {
        return new AlertsStatsRequest();
    }

    @Override
    protected AlertsStatsResponse newResponse() {
        return new AlertsStatsResponse();
    }

    @Override
    protected void masterOperation(AlertsStatsRequest request, ClusterState state, ActionListener<AlertsStatsResponse> listener) throws ElasticsearchException {
        AlertsStatsResponse statsResponse = new AlertsStatsResponse();
        statsResponse.setAlertManagerState(alertsService.state());
        statsResponse.setAlertActionManagerStarted(historyService.started());
        statsResponse.setAlertActionManagerQueueSize(historyService.getQueueSize());
        statsResponse.setNumberOfRegisteredAlerts(alertsService.getNumberOfAlerts());
        statsResponse.setAlertActionManagerLargestQueueSize(historyService.getLargestQueueSize());
        statsResponse.setVersion(AlertsVersion.CURRENT);
        statsResponse.setBuild(AlertsBuild.CURRENT);
        listener.onResponse(statsResponse);
    }

    @Override
    protected ClusterBlockException checkBlock(AlertsStatsRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }


}
