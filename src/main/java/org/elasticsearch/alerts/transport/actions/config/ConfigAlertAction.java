/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.alerts.client.AlertsClientAction;

/**
 * This action deletes an alert from in memory, the scheduler and the index
 */
public class ConfigAlertAction extends AlertsClientAction<ConfigAlertRequest, ConfigAlertResponse, ConfigAlertRequestBuilder> {

    public static final ConfigAlertAction INSTANCE = new ConfigAlertAction();
    public static final String NAME = "indices:data/write/alert/config";

    private ConfigAlertAction() {
        super(NAME);
    }

    @Override
    public ConfigAlertResponse newResponse() {
        return new ConfigAlertResponse();
    }

    @Override
    public ConfigAlertRequestBuilder newRequestBuilder(AlertsClient client) {
        return new ConfigAlertRequestBuilder(client);
    }
}
