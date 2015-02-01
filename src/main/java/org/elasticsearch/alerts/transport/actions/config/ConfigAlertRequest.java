/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.config;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.alerts.ConfigurationService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A delete alert request to delete an alert by name (id)
 */
public class ConfigAlertRequest extends MasterNodeOperationRequest<ConfigAlertRequest> {

    private BytesReference configSource;
    private boolean configSourceUnsafe;

    public ConfigAlertRequest() {
    }

    /**
     * @return The source of the config
     */
    public BytesReference getConfigSource() {
        return configSource;
    }

    /**
     * Sets the source of the config document
     */
    public void setConfigSource(BytesReference configSource) {
        this.configSource = configSource;
        this.configSourceUnsafe = false;
    }

    /**
     * @return Whether the ByteRef configSource safe
     */
    public boolean isConfigSourceUnsafe() {
        return configSourceUnsafe;
    }

    public void setConfigSourceUnsafe(boolean configSourceUnsafe) {
        this.configSourceUnsafe = configSourceUnsafe;
    }


    /**
     * Set the source of the config with boolean to control source safety
     */
    public void setConfigSource(BytesReference configSource, boolean configSourceUnsafe) {
        this.configSource = configSource;
        this.configSourceUnsafe = configSourceUnsafe;
    }


    public void beforeLocalFork() {
        if (configSourceUnsafe) {
            configSource = configSource.copyBytesArray();
            configSourceUnsafe = false;
        }
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (configSource == null){
            validationException = ValidateActions.addValidationError("configName is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        configSource = in.readBytesReference();
        configSourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBytesReference(configSource);
    }

    @Override
    public String toString() {
        return "config {[" + AlertsStore.ALERT_INDEX + "][" + ConfigurationService.CONFIG_TYPE + "]}";
    }
}
