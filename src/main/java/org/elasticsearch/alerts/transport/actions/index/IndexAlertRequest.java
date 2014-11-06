/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.index;


import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class IndexAlertRequest extends MasterNodeOperationRequest<IndexAlertRequest> {

    private String alertName;
    private BytesReference alertSource;

    IndexAlertRequest() {
    }

    public IndexAlertRequest(BytesReference alertSource) {
        this.alertSource = alertSource;
    }

    public String getAlertName() {
        return alertName;
    }

    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    public BytesReference getAlertSource() {
        return alertSource;
    }

    public void setAlertSource(BytesReference alertSource) {
        this.alertSource = alertSource;
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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(alertName);
        out.writeBytesReference(alertSource);
    }

}
