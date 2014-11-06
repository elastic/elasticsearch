/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Performs the get operation.
 */
public class TransportGetAlertAction extends TransportAction<GetAlertRequest,  GetAlertResponse> {

    private final Client client;

    @Inject
    public TransportGetAlertAction(Settings settings, String actionName, ThreadPool threadPool,
                                   ActionFilters actionFilters, Client client) {
        super(settings, actionName, threadPool, actionFilters);
        this.client = client;
    }

    @Override
    protected void doExecute(GetAlertRequest request, ActionListener<GetAlertResponse> listener) {
        try {
            GetResponse getResponse = client.prepareGet(AlertsStore.ALERT_INDEX, AlertsStore.ALERT_TYPE, request.alertName())
                    .setVersion(request.version())
                    .setVersionType(request.versionType()).execute().actionGet();
            GetAlertResponse response = new GetAlertResponse(getResponse);
            listener.onResponse(response);
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }
}
