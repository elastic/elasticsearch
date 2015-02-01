/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.client.Client;

/**
 * A delete document action request builder.
 */
public class DeleteAlertRequestBuilder extends MasterNodeOperationRequestBuilder<DeleteAlertRequest, DeleteAlertResponse, DeleteAlertRequestBuilder, Client> {

    public DeleteAlertRequestBuilder(Client client) {
        super(client, new DeleteAlertRequest());
    }

    public DeleteAlertRequestBuilder(Client client, String alertName) {
        super(client, new DeleteAlertRequest(alertName));
    }

    /**
     * Sets the name of the alert to be deleted
     */
    public DeleteAlertRequestBuilder setAlertName(String alertName) {
        this.request().setAlertName(alertName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<DeleteAlertResponse> listener) {
        new AlertsClient(client).deleteAlert(request, listener);
    }

}
