/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClientInterface;

/**
 * A delete document action request builder.
 */
public class DeleteAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<DeleteAlertRequest, DeleteAlertResponse, DeleteAlertRequestBuilder, AlertsClientInterface> {

    public DeleteAlertRequestBuilder(AlertsClientInterface client) {
        super(client, new DeleteAlertRequest());
    }

    public DeleteAlertRequestBuilder(AlertsClientInterface client, String alertName) {
        super(client, new DeleteAlertRequest(alertName));
    }

    public DeleteAlertRequestBuilder setAlertName(String alertName) {
        this.request().setAlertName(alertName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<DeleteAlertResponse> listener) {
        client.deleteAlert(request, listener);
    }

}
