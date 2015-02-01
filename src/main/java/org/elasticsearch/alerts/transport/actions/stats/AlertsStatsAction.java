/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.alerts.client.AlertsAction;
import org.elasticsearch.client.Client;

/**
 * This Action gets the stats for the alert plugin
 */
public class AlertsStatsAction extends AlertsAction<AlertsStatsRequest, AlertsStatsResponse, AlertsStatsRequestBuilder> {

    public static final AlertsStatsAction INSTANCE = new AlertsStatsAction();
    public static final String NAME = "cluster/alerts/stats";

    private AlertsStatsAction() {
        super(NAME);
    }

    @Override
    public AlertsStatsResponse newResponse() {
        return new AlertsStatsResponse();
    }

    @Override
    public AlertsStatsRequestBuilder newRequestBuilder(Client client) {
        return new AlertsStatsRequestBuilder(client);
    }

}
