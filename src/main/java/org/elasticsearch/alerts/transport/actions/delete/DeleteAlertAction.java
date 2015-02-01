/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.alerts.client.AlertsAction;
import org.elasticsearch.client.Client;

/**
 * This action deletes an alert from in memory, the scheduler and the index
 */
public class DeleteAlertAction extends AlertsAction<DeleteAlertRequest, DeleteAlertResponse, DeleteAlertRequestBuilder> {

    public static final DeleteAlertAction INSTANCE = new DeleteAlertAction();
    public static final String NAME = "indices:data/write/alert/delete";

    private DeleteAlertAction() {
        super(NAME);
    }

    @Override
    public DeleteAlertResponse newResponse() {
        return new DeleteAlertResponse();
    }

    @Override
    public DeleteAlertRequestBuilder newRequestBuilder(Client client) {
        return new DeleteAlertRequestBuilder(client);
    }
}
