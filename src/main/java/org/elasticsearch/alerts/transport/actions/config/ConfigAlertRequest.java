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
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A delete alert request to delete an alert by name (id)
 */
public class ConfigAlertRequest extends MasterNodeOperationRequest<ConfigAlertRequest> {

    private String configName;
    private BytesReference configSource;
    private boolean configSourceUnsafe;


    public ConfigAlertRequest() {
    }

    /**
     * The constructor for the requests that takes the name of the config to modify
     * @param configName
     */
    public ConfigAlertRequest(String configName) {
        this.configName = configName;
    }

    /**
     * The name of the config to be modified
     * @return
     */
    public String getConfigName() {
        return configName;
    }

    /**
     * The name of the config to be modified
     * @param configName
     */
    public void setConfigName(String configName) {
        this.configName = configName;
    }


    /**
     * The source of the config
     * @return
     */
    public BytesReference getConfigSource() {
        return configSource;
    }

    /**
     * The source of the config document
     * @param configSource
     */
    public void setConfigSource(BytesReference configSource) {
        this.configSource = configSource;
        this.configSourceUnsafe = false;
    }

    /**
     * Is the ByteRef configSource safe
     * @return
     */
    public boolean isConfigSourceUnsafe() {
        return configSourceUnsafe;
    }

    public void setConfigSourceUnsafe(boolean configSourceUnsafe) {
        this.configSourceUnsafe = configSourceUnsafe;
    }


    /**
     * Set the source of the config with boolean to control source safety
     * @param configSource
     * @param configSourceUnsafe
     */
    public void setAlertSource(BytesReference configSource, boolean configSourceUnsafe) {
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
        if (configName == null){
            validationException = ValidateActions.addValidationError("configName is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        configName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(configName);
    }

    @Override
    public String toString() {
        return "delete {[" + AlertsStore.ALERT_INDEX + "][" + configName + "]}";
    }
}
