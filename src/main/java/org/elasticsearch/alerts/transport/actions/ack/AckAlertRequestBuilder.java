/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.ack;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;

/**
 * A ack alert action request builder.
 */
public class AckAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<AckAlertRequest, AckAlertResponse, AckAlertRequestBuilder, AlertsClient> {

    public AckAlertRequestBuilder(AlertsClient client) {
        super(client, new AckAlertRequest());
    }

    public AckAlertRequestBuilder(AlertsClient client, String alertName) {
        super(client, new AckAlertRequest(alertName));
    }

    /**
     * Sets the name of the alert to be ack
     * @param alertName
     * @return
     */
    public AckAlertRequestBuilder setAlertName(String alertName) {
        this.request().setAlertName(alertName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<AckAlertResponse> listener) {
        client.ackAlert(request, listener);
    }

}
