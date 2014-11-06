/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.common.bytes.BytesReference;

/**
 */
public class IndexAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<IndexAlertRequest, IndexAlertResponse,
        IndexAlertRequestBuilder, AlertsClientInterface> {


    public IndexAlertRequestBuilder(AlertsClientInterface client) {
        super(client, new IndexAlertRequest());
    }


    public IndexAlertRequestBuilder(AlertsClientInterface client, String alertName) {
        super(client, new IndexAlertRequest());
        request.setAlertName(alertName);
    }

    public IndexAlertRequestBuilder setAlertName(String alertName){
        request.setAlertName(alertName);
        return this;
    }

    public IndexAlertRequestBuilder setAlertSource(BytesReference alertSource) {
        request.setAlertSource(alertSource);
        return this;
    }


    @Override
    protected void doExecute(ActionListener<IndexAlertResponse> listener) {
        client.indexAlert(request, listener);
    }
}
