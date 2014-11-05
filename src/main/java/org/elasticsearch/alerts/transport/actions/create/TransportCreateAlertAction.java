/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.create;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertManager;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.actions.AlertAction;
import org.elasticsearch.alerts.actions.AlertActionManager;
import org.elasticsearch.alerts.triggers.TriggerManager;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.quartz.Trigger;

import java.util.ArrayList;

/**
 */
public class TransportCreateAlertAction extends TransportMasterNodeOperationAction<CreateAlertRequest,  CreateAlertResponse> {

    private final AlertManager alertManager;

    @Inject
    public TransportCreateAlertAction(Settings settings, String actionName, TransportService transportService,
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
    protected CreateAlertRequest newRequest() {
        return new CreateAlertRequest();
    }

    @Override
    protected CreateAlertResponse newResponse() {
        return new CreateAlertResponse();
    }

    @Override
    protected void masterOperation(CreateAlertRequest request, ClusterState state, ActionListener<CreateAlertResponse> listener) throws ElasticsearchException {
        try {
            IndexResponse indexResponse = alertManager.addAlert(request.alert());
            listener.onResponse(new CreateAlertResponse(indexResponse));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(CreateAlertRequest request, ClusterState state) {
        if (!alertManager.isStarted()) {
            return new ClusterBlockException(null);
        }
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, new String[]{AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX});

    }

}
