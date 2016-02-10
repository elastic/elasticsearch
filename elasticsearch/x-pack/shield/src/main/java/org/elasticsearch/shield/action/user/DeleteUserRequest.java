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
 * A request to delete a native user.
 */
public class DeleteUserRequest extends ActionRequest<DeleteUserRequest> implements UserRequest {

    private String username;
    private boolean refresh = true;

    public DeleteUserRequest() {
    }

    public DeleteUserRequest(String username) {
        this.username = username;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("username is missing", validationException);
        }
        return validationException;
    }

    public String username() {
        return this.username;
    }

    public boolean refresh() {
        return refresh;
    }

    public void username(String username) {
        this.username = username;
    }

    public void refresh(boolean refresh) {
        this.refresh = refresh;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        username = in.readString();
        refresh = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeBoolean(refresh);
    }

}
