/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.get;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientInterface;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.VersionType;

/**
 * A delete document action request builder.
 */
public class GetAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<GetAlertRequest, GetAlertResponse, GetAlertRequestBuilder, AlertsClientInterface> {


    public GetAlertRequestBuilder(AlertsClientInterface client, String alertName) {
        super(client, new GetAlertRequest(alertName));
    }


    public GetAlertRequestBuilder(AlertsClientInterface client) {
        super(client, new GetAlertRequest());
    }

    public GetAlertRequestBuilder setAlertName(String alertName) {
        request.alertName(alertName);
        return this;
    }

    /**
     * Sets the type of versioning to use. Defaults to {@link org.elasticsearch.index.VersionType#INTERNAL}.
     */
    public GetAlertRequestBuilder setVersionType(VersionType versionType) {
        request.versionType(versionType);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<GetAlertResponse> listener) {
        client.getAlert(request, listener);
    }
}
