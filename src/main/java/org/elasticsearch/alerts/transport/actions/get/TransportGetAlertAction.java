/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeOperationAction;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.AlertsService;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

/**
 * Performs the get operation.
 */
public class TransportGetAlertAction extends TransportMasterNodeOperationAction<GetAlertRequest,  GetAlertResponse> {

    private final AlertsService alertsService;

    @Inject
    public TransportGetAlertAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, AlertsService alertsService) {
        super(settings, GetAlertAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.alertsService = alertsService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME; // Super lightweight operation, so don't fork
    }

    @Override
    protected GetAlertRequest newRequest() {
        return new GetAlertRequest();
    }

    @Override
    protected GetAlertResponse newResponse() {
        return new GetAlertResponse();
    }

    @Override
    protected void masterOperation(GetAlertRequest request, ClusterState state, ActionListener<GetAlertResponse> listener) throws ElasticsearchException {
        Alert alert = alertsService.getAlert(request.alertName());
        GetResult getResult;
        if (alert != null) {
            BytesReference alertSource = null;
            try (XContentBuilder builder = XContentBuilder.builder(alert.getContentType().xContent())) {
                builder.value(alert);
                alertSource = builder.bytes();
            } catch (IOException e) {
                listener.onFailure(e);
            }
            getResult = new GetResult(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, alert.getAlertName(), alert.getVersion(), true, alertSource, null);
        } else {
            getResult = new GetResult(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, request.alertName(), -1, false, null, null);
        }
        listener.onResponse(new GetAlertResponse(new GetResponse(getResult)));
    }

    @Override
    protected ClusterBlockException checkBlock(GetAlertRequest request, ClusterState state) {
        return state.blocks().indexBlockedException(ClusterBlockLevel.READ, AlertsStore.ALERT_INDEX);
    }
}
