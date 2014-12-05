/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.ack;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.AlertManager;
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
public class TransportAckAlertAction extends TransportMasterNodeOperationAction<AckAlertRequest, AckAlertResponse> {

    private final AlertManager alertManager;

    @Inject
    public TransportAckAlertAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, AlertManager alertManager) {
        super(settings, AckAlertAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.alertManager = alertManager;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected AckAlertRequest newRequest() {
        return new AckAlertRequest();
    }

    @Override
    protected AckAlertResponse newResponse() {
        return new AckAlertResponse();
    }

    @Override
    protected void masterOperation(AckAlertRequest request, ClusterState state, ActionListener<AckAlertResponse> listener) throws ElasticsearchException {
        try {
            AckAlertResponse response = new AckAlertResponse(alertManager.ackAlert(request.getAlertName()));
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(AckAlertRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, AlertsStore.ALERT_INDEX);
    }


}
