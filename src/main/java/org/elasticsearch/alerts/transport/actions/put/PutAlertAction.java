/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.put;

import org.elasticsearch.alerts.client.AlertsAction;
import org.elasticsearch.client.Client;

/**
 * This action puts an alert into the alert index and adds it to the scheduler
 */
public class PutAlertAction extends AlertsAction<PutAlertRequest, PutAlertResponse, PutAlertRequestBuilder> {

    public static final PutAlertAction INSTANCE = new PutAlertAction();
    public static final String NAME = "indices:data/write/alert/put";

    private PutAlertAction() {
        super(NAME);
    }

    @Override
    public PutAlertRequestBuilder newRequestBuilder(Client client) {
        return new PutAlertRequestBuilder(client);
    }

    @Override
    public PutAlertResponse newResponse() {
        return new PutAlertResponse();
    }
}
