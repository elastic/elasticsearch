/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class AuthenticateRequest extends ActionRequest implements UserRequest {

    private String username;

    public AuthenticateRequest(StreamInput in) throws IOException {
        super(in);
        username = in.readString();
    }

    public AuthenticateRequest() {}

    public AuthenticateRequest(String username) {
        this.username = username;
    }

    @Override
    public ActionRequestValidationException validate() {
        // we cannot apply our validation rules here as an authenticate request could be for an LDAP user that doesn't fit our restrictions
        return null;
    }

    public String username() {
        return username;
    }

    public void username(String username) {
        this.username = username;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
    }
}
