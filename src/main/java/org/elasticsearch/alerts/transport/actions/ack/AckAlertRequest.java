/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.ack;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A delete alert request to delete an alert by name (id)
 */
public class AckAlertRequest extends MasterNodeOperationRequest<AckAlertRequest> {

    private String alertName;

    public AckAlertRequest() {
    }

    public AckAlertRequest(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return The name of the alert to be acked
     */
    public String getAlertName() {
        return alertName;
    }

    /**
     * Sets the name of the alert to be acked
     */
    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (alertName == null){
            validationException = ValidateActions.addValidationError("alertName is missing", validationException);
        }
        return validationException;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        alertName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(alertName);
    }

    @Override
    public String toString() {
        return "ack {[" + AlertsStore.ALERT_INDEX + "][" +  alertName + "]}";
    }
}
