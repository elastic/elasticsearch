/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.client.Client;

/**
 */
public class CreateAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<CreateAlertRequest, CreateAlertResponse,
        CreateAlertRequestBuilder, AlertsClientInterface> {


    public CreateAlertRequestBuilder(AlertsClientInterface client) {
        super(client, new CreateAlertRequest(null));
    }


    public CreateAlertRequestBuilder(AlertsClientInterface client, Alert alert) {
        super(client, new CreateAlertRequest(alert));
    }

    @Override
    protected void doExecute(ActionListener<CreateAlertResponse> listener) {
        client.createAlert(request, listener);
    }
}
