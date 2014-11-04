/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.transport.actions.update;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ValidateActions;
import org.elasticsearch.action.support.master.MasterNodeOperationRequest;
import org.elasticsearch.alerts.Alert;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 */
public class UpdateAlertRequest extends MasterNodeOperationRequest<UpdateAlertRequest> {

    private Alert alert;


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (alert == null) {
            validationException = ValidateActions.addValidationError("alert is missing", validationException);
        }
        return validationException;
    }

    UpdateAlertRequest() {
    }

    public UpdateAlertRequest(Alert alert) {
        this.alert = alert;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        alert = new Alert();
        alert.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        alert.writeTo(out);
    }


    public Alert alert() {
        return alert;
    }

    public void Alert(Alert alert) {
        this.alert = alert;
    }



}
