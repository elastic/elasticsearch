/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;

/**
 * A alert config action request builder.
 */
public class ConfigAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<ConfigAlertRequest, ConfigAlertResponse, ConfigAlertRequestBuilder, AlertsClient> {

    public ConfigAlertRequestBuilder(AlertsClient client) {
        super(client, new ConfigAlertRequest());
    }

    public ConfigAlertRequestBuilder(AlertsClient client, String alertName) {
        super(client, new ConfigAlertRequest(alertName));
    }

    /**
     * Sets the name of the config to be modified
     * @param configName
     * @return
     */
    public ConfigAlertRequestBuilder setAlertName(String configName) {
        this.request().setConfigName(configName);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<ConfigAlertResponse> listener) {
        client.alertConfig(request, listener);
    }

}
