/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.AlertService;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Performs the delete operation.
 */
public class TransportDeleteAlertAction extends TransportMasterNodeOperationAction<DeleteAlertRequest,  DeleteAlertResponse> {

    private final AlertService alertService;

    @Inject
    public TransportDeleteAlertAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                      ThreadPool threadPool, ActionFilters actionFilters, AlertService alertService) {
        super(settings, DeleteAlertAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.alertService = alertService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected DeleteAlertRequest newRequest() {
        return new DeleteAlertRequest();
    }

    @Override
    protected DeleteAlertResponse newResponse() {
        return new DeleteAlertResponse();
    }

    @Override
    protected void masterOperation(DeleteAlertRequest request, ClusterState state, ActionListener<DeleteAlertResponse> listener) throws ElasticsearchException {
        try {
            DeleteAlertResponse response = new DeleteAlertResponse(alertService.deleteAlert(request.getAlertName()));
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteAlertRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, AlertsStore.ALERT_INDEX);
    }


}
