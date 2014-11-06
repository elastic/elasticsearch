/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.index;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexResponse;
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
 */
public class TransportIndexAlertAction extends TransportMasterNodeOperationAction<IndexAlertRequest, IndexAlertResponse> {

    private final AlertManager alertManager;

    @Inject
    public TransportIndexAlertAction(Settings settings, String actionName, TransportService transportService,
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
    protected IndexAlertRequest newRequest() {
        return new IndexAlertRequest();
    }

    @Override
    protected IndexAlertResponse newResponse() {
        return new IndexAlertResponse();
    }

    @Override
    protected void masterOperation(IndexAlertRequest request, ClusterState state, ActionListener<IndexAlertResponse> listener) throws ElasticsearchException {
        try {
            IndexResponse indexResponse = alertManager.addAlert(request.getAlertName(), request.getAlertSource());
            listener.onResponse(new IndexAlertResponse(indexResponse));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected ClusterBlockException checkBlock(IndexAlertRequest request, ClusterState state) {
        return state.blocks().indicesBlockedException(ClusterBlockLevel.WRITE, new String[]{AlertsStore.ALERT_INDEX, AlertActionManager.ALERT_HISTORY_INDEX});

    }

}
