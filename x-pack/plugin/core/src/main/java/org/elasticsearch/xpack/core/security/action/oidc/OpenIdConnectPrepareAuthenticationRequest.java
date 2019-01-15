/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to prepare an OAuth 2.0 authorization request
 */
public class OpenIdConnectPrepareAuthenticationRequest extends ActionRequest {

    private String realmName;

    public String getRealmName() {
        return realmName;
    }

    public void setRealmName(String realmName) {
        this.realmName = realmName;
    }

    public OpenIdConnectPrepareAuthenticationRequest() {
    }

    public OpenIdConnectPrepareAuthenticationRequest(StreamInput in) throws IOException {
        super.readFrom(in);
        realmName = in.readString();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(realmName) == false) {
            validationException = addValidationError("realm name must be provided", null);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(realmName);
    }

    @Override
    public void readFrom(StreamInput in) {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    public String toString() {
        return "{realmName=" + realmName + "}";
    }

}
