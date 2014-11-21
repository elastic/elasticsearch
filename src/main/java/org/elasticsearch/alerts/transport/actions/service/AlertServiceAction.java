/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.service;

import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientAction;

/**
 */
public class AlertServiceAction  extends AlertsClientAction<AlertsServiceRequest, AlertsServiceResponse, AlertServiceRequestBuilder> {

    public static final AlertServiceAction INSTANCE = new AlertServiceAction();
    public static final String NAME = "cluster:admin/alerts/service";

    private AlertServiceAction() {
        super(NAME);
    }

    @Override
    public AlertsServiceResponse newResponse() {
        return new AlertsServiceResponse();
    }

    @Override
    public AlertServiceRequestBuilder newRequestBuilder(AlertsClient client) {
        return new AlertServiceRequestBuilder(client);
    }
}
