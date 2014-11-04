/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.create;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientAction;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.client.Client;

/**
 */
public class CreateAlertAction extends AlertsClientAction<CreateAlertRequest, CreateAlertResponse, CreateAlertRequestBuilder> {

    public static final CreateAlertAction INSTANCE = new CreateAlertAction();
    public static final String NAME = "indices:data/write/alert/create";

    private CreateAlertAction() {
        super(NAME);
    }


    @Override
    public CreateAlertRequestBuilder newRequestBuilder(AlertsClientInterface client) {
        return new CreateAlertRequestBuilder(client);
    }

    @Override
    public CreateAlertResponse newResponse() {
        return new CreateAlertResponse();
    }
}
