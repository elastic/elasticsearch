/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.put;


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * This request class contains the data needed to create an alert along with the name of the alert
 * the name of the alert will become the ID of the indexed document.
 */
public class PutAlertRequest extends MasterNodeOperationRequest<PutAlertRequest> {

    private String alertName;
    private BytesReference alertSource;
    private boolean alertSourceUnsafe;

    public PutAlertRequest() {
    }

    /**
     * @param alertSource The alertSource
     */
    public PutAlertRequest(BytesReference alertSource) {
        this.alertSource = alertSource;
    }

    /**
     * @return The name that will be the ID of the indexed document
     */
    public String getAlertName() {
        return alertName;
    }

    /**
     * Set the alert name
     */
    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return The source of the alert
     */
    public BytesReference getAlertSource() {
        return alertSource;
    }

    /**
     * Set the source of the alert
     */
    public void setAlertSource(BytesReference alertSource) {
        this.alertSource = alertSource;
        this.alertSourceUnsafe = false;
    }

    /**
     * Set the source of the alert with boolean to control source safety
     */
    public void setAlertSource(BytesReference alertSource, boolean alertSourceUnsafe) {
        this.alertSource = alertSource;
        this.alertSourceUnsafe = alertSourceUnsafe;
    }

    public void beforeLocalFork() {
        if (alertSourceUnsafe) {
            alertSource = alertSource.copyBytesArray();
            alertSourceUnsafe = false;
        }
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (alertName == null) {
            validationException = ValidateActions.addValidationError("alertName is missing", validationException);
        }
        if (alertSource == null) {
            validationException = ValidateActions.addValidationError("alertSource is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        alertName = in.readString();
        alertSource = in.readBytesReference();
        alertSourceUnsafe = false;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(alertName);
        out.writeBytesReference(alertSource);
    }

}
