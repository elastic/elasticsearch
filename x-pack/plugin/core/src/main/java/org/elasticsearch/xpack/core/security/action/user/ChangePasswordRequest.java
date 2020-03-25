/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.CharArrays;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request to change a user's password.
 */
public class ChangePasswordRequest extends ActionRequest
        implements UserRequest, WriteRequest<ChangePasswordRequest> {

    private String username;
    private char[] passwordHash;
    private RefreshPolicy refreshPolicy = RefreshPolicy.IMMEDIATE;

    public ChangePasswordRequest() {}

    public ChangePasswordRequest(StreamInput in) throws IOException {
        super(in);
        username = in.readString();
        passwordHash = CharArrays.utf8BytesToChars(BytesReference.toBytes(in.readBytesReference()));
        refreshPolicy = RefreshPolicy.readFrom(in);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (username == null) {
            validationException = addValidationError("username is missing", validationException);
        }
        if (passwordHash == null) {
            validationException = addValidationError("password is missing", validationException);
        }
        return validationException;
    }

    public String username() {
        return username;
    }

    public void username(String username) {
        this.username = username;
    }

    public char[] passwordHash() {
        return passwordHash;
    }

    public void passwordHash(char[] passwordHash) {
        this.passwordHash = passwordHash;
    }

    /**
     * Should this request trigger a refresh ({@linkplain RefreshPolicy#IMMEDIATE}, the default), wait for a refresh (
     * {@linkplain RefreshPolicy#WAIT_UNTIL}), or proceed ignore refreshes entirely ({@linkplain RefreshPolicy#NONE}).
     */
    @Override
    public RefreshPolicy getRefreshPolicy() {
        return refreshPolicy;
    }

    @Override
    public ChangePasswordRequest setRefreshPolicy(RefreshPolicy refreshPolicy) {
        this.refreshPolicy = refreshPolicy;
        return this;
    }

    @Override
    public String[] usernames() {
        return new String[] { username };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeBytesReference(new BytesArray(CharArrays.toUtf8Bytes(passwordHash)));
        refreshPolicy.writeTo(out);
    }
}
