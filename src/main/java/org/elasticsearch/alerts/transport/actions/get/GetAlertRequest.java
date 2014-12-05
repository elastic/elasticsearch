/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.alerts.AlertsStore;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.index.VersionType;

import java.io.IOException;

/**
 * The request to get the alert by name (id)
 */
public class GetAlertRequest extends MasterNodeOperationRequest<GetAlertRequest> {

    private String alertName;
    private long version = Versions.MATCH_ANY;
    private VersionType versionType = VersionType.INTERNAL;


    public GetAlertRequest() {
    }

    /**
     * Constructor taking name (id) of the alert to retrieve
     * @param alertName
     */
    public GetAlertRequest(String alertName) {
        this.alertName = alertName;
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (alertName == null) {
            validationException = ValidateActions.addValidationError("alertName is missing", validationException);
        }
        return validationException;
    }


    /**
     * The name of the alert to retrieve
     * @return
     */
    public String alertName() {
        return alertName;
    }

    public GetAlertRequest alertName(String alertName){
        this.alertName = alertName;
        return this;
    }

    /**
     * Sets the version, which will cause the delete operation to only be performed if a matching
     * version exists and no changes happened on the doc since then.
     */
    public GetAlertRequest version(long version) {
        this.version = version;
        return this;
    }

    public long version() {
        return this.version;
    }

    public GetAlertRequest versionType(VersionType versionType) {
        this.versionType = versionType;
        return this;
    }

    public VersionType versionType() {
        return this.versionType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        version = Versions.readVersion(in);
        versionType = VersionType.fromValue(in.readByte());
        alertName = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        Versions.writeVersion(version, out);
        out.writeByte(versionType.getValue());
        out.writeString(alertName);
    }

    @Override
    public String toString() {
        return "delete {[" + AlertsStore.ALERT_INDEX + "][" + alertName +"]}";
    }
}
