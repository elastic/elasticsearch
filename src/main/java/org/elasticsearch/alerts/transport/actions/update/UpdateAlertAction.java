/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.update;

import org.elasticsearch.action.ClientAction;
import org.elasticsearch.alerts.client.AlertsClientAction;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.client.Client;

/**
 */
public class UpdateAlertAction extends AlertsClientAction<UpdateAlertRequest, UpdateAlertResponse, UpdateAlertRequestBuilder> {

    public static final UpdateAlertAction INSTANCE = new UpdateAlertAction();
    public static final String NAME = "indices:data/write/alert/update";

    private UpdateAlertAction() {
        super(NAME);
    }


    @Override
    public UpdateAlertRequestBuilder newRequestBuilder(AlertsClientInterface client) {
        return new UpdateAlertRequestBuilder(client);
    }

    @Override
    public UpdateAlertResponse newResponse() {
        return new UpdateAlertResponse();
    }

}
