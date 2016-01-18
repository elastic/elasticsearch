/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.admin.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to retrieve a user from the shield administrative index from a username
 */
public class GetUsersRequest extends ActionRequest<GetUsersRequest> {

    private String[] users;

    public GetUsersRequest() {
        users = Strings.EMPTY_ARRAY;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (users == null) {
            validationException = addValidationError("users cannot be null", validationException);
        }
        return validationException;
    }

    public void users(String... usernames) {
        this.users = usernames;
    }

    public String[] users() {
        return users;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        users = in.readStringArray();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(users);
    }

}
