/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.update;

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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportUpdateAlertAction extends TransportMasterNodeOperationAction<UpdateAlertRequest, UpdateAlertResponse> {

    private final AlertManager alertManager;

    public TransportUpdateAlertAction(Settings settings, String actionName, TransportService transportService,
                                      ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters,
                                      AlertManager alertManager) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters);
        this.alertManager = alertManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected UpdateAlertRequest newRequest() {
        return new UpdateAlertRequest();
    }

    @Override
    protected UpdateAlertResponse newResponse() {
        return new UpdateAlertResponse();
    }

    @Override
    protected void masterOperation(UpdateAlertRequest request, ClusterState state, ActionListener<UpdateAlertResponse> listener) throws ElasticsearchException {
        try {
            boolean success = alertManager.updateAlert(request.alert());
            listener.onResponse(new UpdateAlertResponse(success));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(UpdateAlertRequest request, ClusterState state) {
        if (!alertManager.isStarted()) {
            return new ClusterBlockException(null);
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, new String[]{AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX});
    }

}
