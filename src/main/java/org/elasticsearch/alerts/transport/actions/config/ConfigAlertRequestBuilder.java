/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.MasterNodeOperationRequestBuilder;
import org.elasticsearch.alerts.client.AlertsClient;
import org.elasticsearch.common.bytes.BytesReference;

/**
 * A alert config action request builder.
 */
public class ConfigAlertRequestBuilder
        extends MasterNodeOperationRequestBuilder<ConfigAlertRequest, ConfigAlertResponse, ConfigAlertRequestBuilder, AlertsClient> {

    public ConfigAlertRequestBuilder(AlertsClient client) {
        super(client, new ConfigAlertRequest());
    }

    /**
     * Sets the source of the config to be modified
     * @param configSource
     * @return
     */
    public ConfigAlertRequestBuilder setConfigSource(BytesReference configSource) {
        this.request().setConfigSource(configSource);
        return this;
    }

    /**
     * Sets the source of the config to be modified with boolean to control safety
     * @param configSource
     * @return
     */
    public ConfigAlertRequestBuilder setConfigSource(BytesReference configSource, boolean sourceUnsafe) {
        this.request().setConfigSource(configSource);
        this.request().setConfigSourceUnsafe(sourceUnsafe);
        return this;
    }

    @Override
    protected void doExecute(final ActionListener<ConfigAlertResponse> listener) {
        client.alertConfig(request, listener);
    }

}
