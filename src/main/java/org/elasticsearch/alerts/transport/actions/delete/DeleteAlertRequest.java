/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.delete;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;

import java.io.IOException;

/**
 * A delete alert request to delete an alert by name (id)
 */
public class DeleteAlertRequest extends MasterNodeOperationRequest<DeleteAlertRequest> {

    private String alertName;
    private long version = Versions.MATCH_ANY;

    public DeleteAlertRequest() {
    }

    public DeleteAlertRequest(String alertName) {
        this.alertName = alertName;
    }

    /**
     * @return The name of the alert to be deleted
     */
    public String getAlertName() {
        return alertName;
    }

    /**
     * Sets the name of the alert to be deleted
     */
    public void setAlertName(String alertName) {
        this.alertName = alertName;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
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
        version = Versions.readVersion(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(alertName);
        Versions.writeVersion(version, out);
    }

    @Override
    public String toString() {
        return "delete {[" + AlertsStore.ALERT_INDEX + "][" +  alertName + "]}";
    }
}
