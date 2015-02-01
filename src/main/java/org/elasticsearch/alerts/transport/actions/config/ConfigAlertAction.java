/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.alerts.client.AlertsAction;
import org.elasticsearch.client.Client;

/**
 * This action deletes an alert from in memory, the scheduler and the index
 */
public class ConfigAlertAction extends AlertsAction<ConfigAlertRequest, ConfigAlertResponse, ConfigAlertRequestBuilder> {

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
    public ConfigAlertRequestBuilder newRequestBuilder(Client client) {
        return new ConfigAlertRequestBuilder(client);
    }

}
