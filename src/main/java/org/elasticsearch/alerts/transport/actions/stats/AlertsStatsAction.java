/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.stats;

import org.elasticsearch.alerts.client.AlertsClientAction;
import org.elasticsearch.alerts.client.AlertsClient;

/**
 */
public class AlertsStatsAction extends AlertsClientAction<AlertsStatsRequest, AlertsStatsResponse, AlertsStatsRequestBuilder> {

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
    public AlertsStatsRequestBuilder newRequestBuilder(AlertsClient client) {
        return new AlertsStatsRequestBuilder(client);
    }
}
