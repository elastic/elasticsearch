/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.service;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.client.Client;

/**
 */
public class AlertsServiceRequestBuilder extends MasterNodeOperationRequestBuilder<AlertsServiceRequest, AlertsServiceResponse, AlertsServiceRequestBuilder, Client> {

    public AlertsServiceRequestBuilder(Client client) {
        super(client, new AlertsServiceRequest());
    }

    /**
     * Starts alerting if not already started.
     */
    public AlertsServiceRequestBuilder start() {
        request.start();
        return this;
    }

    /**
     * Stops alerting if not already stopped.
     */
    public AlertsServiceRequestBuilder stop() {
        request.stop();
        return this;
    }

    /**
     * Starts and stops alerting.
     */
    public AlertsServiceRequestBuilder restart() {
        request.restart();
        return this;
    }

    @Override
    protected void doExecute(ActionListener<AlertsServiceResponse> listener) {
        new AlertsClient(client).alertService(request, listener);
    }
}
