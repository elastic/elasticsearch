/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.put;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
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
 */
public class TransportPutAlertAction extends TransportMasterNodeOperationAction<PutAlertRequest, PutAlertResponse> {

    private final AlertService alertService;

    @Inject
    public TransportPutAlertAction(Settings settings, TransportService transportService, ClusterService clusterService,
                                   ThreadPool threadPool, ActionFilters actionFilters, AlertService alertService) {
        super(settings, PutAlertAction.NAME, transportService, clusterService, threadPool, actionFilters);
        this.alertService = alertService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected PutAlertRequest newRequest() {
        return new PutAlertRequest();
    }

    @Override
    protected PutAlertResponse newResponse() {
        return new PutAlertResponse();
    }

    @Override
    protected void masterOperation(PutAlertRequest request, ClusterState state, ActionListener<PutAlertResponse> listener) throws ElasticsearchException {
        try {
            IndexResponse indexResponse = alertService.putAlert(request.getAlertName(), request.getAlertSource());
            listener.onResponse(new PutAlertResponse(indexResponse));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(PutAlertRequest request, ClusterState state) {
        request.beforeLocalFork(); // This is the best place to make the alert source safe
        return state.blocks().indexBlockedException(ClusterBlockLevel.WRITE, AlertsStore.ALERT_INDEX);
    }

}
