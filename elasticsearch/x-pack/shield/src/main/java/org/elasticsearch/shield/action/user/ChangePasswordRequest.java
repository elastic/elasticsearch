/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.action.user;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.shield.authc.support.CharArrays;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 */
public class ChangePasswordRequest extends ActionRequest<ChangePasswordRequest> implements UserRequest {

    private String username;
    private char[] passwordHash;
    private boolean refresh = true;

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

    public boolean refresh() {
        return refresh;
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
        passwordHash = CharArrays.utf8BytesToChars(in.readBytesReference().array());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(username);
        out.writeBytesReference(new BytesArray(CharArrays.toUtf8Bytes(passwordHash)));
    }
}
