/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;


import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import static org.elasticsearch.action.ValidateActions.addValidationError;

import java.io.IOException;

public class SamlInitiateSingleSignOnRequest extends ActionRequest {

    private String spEntityId;

    public SamlInitiateSingleSignOnRequest(StreamInput in) throws IOException {
        super(in);
        spEntityId = in.readString();
    }

    public SamlInitiateSingleSignOnRequest() {
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(spEntityId)) {
            validationException = addValidationError("sp_entity_id is missing", validationException);
        }
        return validationException;
    }

    public String getSpEntityId() {
        return spEntityId;
    }

    public void setSpEntityId(String spEntityId) {
        this.spEntityId = spEntityId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(spEntityId);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{spEntityId='" + spEntityId + "'}";
    }
}
