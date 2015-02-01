/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A Builder to build a PutAlertRequest
 */
public class PutAlertRequestBuilder extends MasterNodeOperationRequestBuilder<PutAlertRequest, PutAlertResponse, PutAlertRequestBuilder, Client> {

    public PutAlertRequestBuilder(Client client) {
        super(client, new PutAlertRequest());
    }

    public PutAlertRequestBuilder(Client client, String alertName) {
        super(client, new PutAlertRequest());
        request.setAlertName(alertName);
    }

    /**
     * @param alertName The alert name to be created
     */
    public PutAlertRequestBuilder setAlertName(String alertName){
        request.setAlertName(alertName);
        return this;
    }

    /**
     * @param alertSource the source of the alert to be created
     */
    public PutAlertRequestBuilder setAlertSource(BytesReference alertSource) {
        request.setAlertSource(alertSource);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<PutAlertResponse> listener) {
        new AlertsClient(client).putAlert(request, listener);
    }
}
