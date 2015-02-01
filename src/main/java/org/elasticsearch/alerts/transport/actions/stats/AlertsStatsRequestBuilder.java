/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.client.Client;

/**
 * An alert stats document action request builder.
 */
public class AlertsStatsRequestBuilder extends MasterNodeOperationRequestBuilder<AlertsStatsRequest, AlertsStatsResponse, AlertsStatsRequestBuilder, Client> {

    /**
     * The constructor for the AlertsStatsRequestBuilder
     */
    public AlertsStatsRequestBuilder(Client client) {
        super(client, new AlertsStatsRequest());
    }


    @Override
    protected void doExecute(final ActionListener<AlertsStatsResponse> listener) {
        new AlertsClient(client).alertsStats(request, listener);
    }

}
