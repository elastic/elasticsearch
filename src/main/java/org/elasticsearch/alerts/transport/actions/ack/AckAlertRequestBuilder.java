/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.ack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.client.Client;

/**
 * A ack alert action request builder.
 */
public class AckAlertRequestBuilder extends MasterNodeOperationRequestBuilder<AckAlertRequest, AckAlertResponse, AckAlertRequestBuilder, Client> {

    public AckAlertRequestBuilder(Client client) {
        super(client, new AckAlertRequest());
    }

    public AckAlertRequestBuilder(Client client, String alertName) {
        super(client, new AckAlertRequest(alertName));
    }

    /**
     * Sets the name of the alert to be ack
     */
    public AckAlertRequestBuilder setAlertName(String alertName) {
        this.request().setAlertName(alertName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<AckAlertResponse> listener) {
        new AlertsClient(client).ackAlert(request, listener);
    }

}
