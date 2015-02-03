/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.service;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 */
public class TransportAlertsServiceAction extends TransportMasterNodeOperationAction<AlertsServiceRequest, AlertsServiceResponse> {

    private final AlertsService alertsService;

    @Inject
    public TransportAlertsServiceAction(Settings settings, String actionName, TransportService transportService, ClusterService clusterService, ThreadPool threadPool, ActionFilters actionFilters, AlertsService alertsService) {
        super(settings, actionName, transportService, clusterService, threadPool, actionFilters);
        this.alertsService = alertsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AlertsServiceRequest newRequest() {
        return new AlertsServiceRequest();
    }

    @Override
    protected AlertsServiceResponse newResponse() {
        return new AlertsServiceResponse();
    }

    @Override
    protected void masterOperation(AlertsServiceRequest request, ClusterState state, ActionListener<AlertsServiceResponse> listener) throws ElasticsearchException {
        switch (request.getCommand()) {
            case "start":
                alertsService.start();
                break;
            case "stop":
                alertsService.stop();
                break;
            case "restart":
                alertsService.start();
                alertsService.stop();
                break;
            default:
                listener.onFailure(new ElasticsearchIllegalArgumentException("Command [" + request.getCommand() + "] is undefined"));
                return;
        }
        listener.onResponse(new AlertsServiceResponse(true));
    }

    @Override
    protected ClusterBlockException checkBlock(AlertsServiceRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA);
    }
}
