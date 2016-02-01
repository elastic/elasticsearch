/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to delete a user from the shield administrative index by username
 */
public class DeleteUserRequest extends ActionRequest<DeleteUserRequest> {

    private String username;

    public DeleteUserRequest() {
    }

    public DeleteUserRequest(String user) {
        this.username = user;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("user is missing", validationException);
        }
        return validationException;
    }

    public String user() {
        return this.username;
    }

    public void user(String username) {
        this.username = username;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        username = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
    }

}
