/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for invalidating a token so that it can no longer be used
 */
public final class InvalidateTokenRequest extends ActionRequest {

    private String tokenString;

    InvalidateTokenRequest() {}

    /**
     * @param tokenString the string representation of the token
     */
    public InvalidateTokenRequest(String tokenString) {
        this.tokenString = tokenString;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.isNullOrEmpty(tokenString)) {
            validationException = addValidationError("token string must be provided", null);
        }
        return validationException;
    }

    String getTokenString() {
        return tokenString;
    }

    void setTokenString(String token) {
        this.tokenString = token;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(tokenString);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        tokenString = in.readString();
    }
}
