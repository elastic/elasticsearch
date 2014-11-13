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
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.actions.AlertActionManager;
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
public class TransportAlertStatsAction extends TransportMasterNodeOperationAction<AlertsStatsRequest, AlertsStatsResponse> {

    private final AlertManager alertManager;
    private final AlertActionManager alertActionManager;

    @Inject
    public TransportAlertStatsAction(Settings settings, String actionName, TransportService transportService,
                                     ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                     AlertManager alertManager, AlertActionManager alertActionManager) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters);
        this.alertManager = alertManager;
        this.alertActionManager = alertActionManager;
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
        statsResponse.setAlertManagerStarted(alertManager.isStarted());
        statsResponse.setAlertActionManagerStarted(alertActionManager.started());
        statsResponse.setAlertActionManagerQueueSize(alertActionManager.getQueueSize());
        statsResponse.setNumberOfRegisteredAlerts(alertManager.getNumberOfAlerts());
    }

    @Override
    protected ClusterBlockException checkBlock(AlertsStatsRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, new String[]{AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX});
    }


}
