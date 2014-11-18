/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A Builder to build a PutAlertRequest
 */
public class PutAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<PutAlertRequest, PutAlertResponse,
        PutAlertRequestBuilder, AlertsClient> {

    /**
     * The Constructor for the PutAlertRequestBuilder
     * @param client The client that will execute the action
     */
    public PutAlertRequestBuilder(AlertsClient client) {
        super(client, new PutAlertRequest());
    }

    /**
     * The Constructor for the PutAlertRequestBuilder
     * @param client The client that will execute the action
     * @param alertName The name of the alert to be put
     */
    public PutAlertRequestBuilder(AlertsClient client, String alertName) {
        super(client, new PutAlertRequest());
        request.setAlertName(alertName);
    }

    /**
     * Sets the alert name to be created
     * @param alertName
     * @return
     */
    public PutAlertRequestBuilder setAlertName(String alertName){
        request.setAlertName(alertName);
        return this;
    }

    /**
     * Sets the source of the alert to be created
     * @param alertSource
     * @return
     */
    public PutAlertRequestBuilder setAlertSource(BytesReference alertSource) {
        request.setAlertSource(alertSource);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<PutAlertResponse> listener) {
        client.indexAlert(request, listener);
    }
}
