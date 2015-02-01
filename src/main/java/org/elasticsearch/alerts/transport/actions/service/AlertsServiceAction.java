/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.service;

import org.elasticsearch.alerts.client.AlertsAction;
import org.elasticsearch.client.Client;

/**
 */
public class AlertsServiceAction extends AlertsAction<AlertsServiceRequest, AlertsServiceResponse, AlertsServiceRequestBuilder> {

    public static final AlertsServiceAction INSTANCE = new AlertsServiceAction();
    public static final String NAME = "cluster:admin/alerts/service";

    private AlertsServiceAction() {
        super(NAME);
    }

    @Override
    public AlertsServiceResponse newResponse() {
        return new AlertsServiceResponse();
    }

    @Override
    public AlertsServiceRequestBuilder newRequestBuilder(Client client) {
        return new AlertsServiceRequestBuilder(client);
    }

}
