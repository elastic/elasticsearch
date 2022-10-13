/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class SamlValidateAuthnRequestRequest extends ActionRequest {

    private String queryString;

    public SamlValidateAuthnRequestRequest(StreamInput in) throws IOException {
        super(in);
        queryString = in.readString();
    }

    public SamlValidateAuthnRequestRequest() {}

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(queryString)) {
            validationException = addValidationError("Authentication request query string must be provided", validationException);
        }
        return validationException;
    }

    public String getQueryString() {
        return queryString;
    }

    public void setQueryString(String queryString) {
        this.queryString = queryString;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(queryString);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "{queryString='" + queryString + "'}";
    }
}
