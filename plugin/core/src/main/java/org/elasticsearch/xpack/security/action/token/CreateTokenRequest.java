/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.security.authc.support.CharArrays;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to create a token based on the provided information. This class accepts the
 * fields for an OAuth 2.0 access token request that uses the <code>password</code> grant type.
 */
public final class CreateTokenRequest extends ActionRequest {

    private String grantType;
    private String username;
    private SecureString password;
    private String scope;

    CreateTokenRequest() {}

    public CreateTokenRequest(String grantType, String username, SecureString password, @Nullable String scope) {
        this.grantType = grantType;
        this.username = username;
        this.password = password;
        this.scope = scope;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ("password".equals(grantType) == false) {
            validationException = addValidationError("only [password] grant_type is supported", validationException);
        }
        if (Strings.isNullOrEmpty(username)) {
            validationException = addValidationError("username is missing", validationException);
        }
        if (password == null || password.getChars() == null || password.getChars().length == 0) {
            validationException = addValidationError("password is missing", validationException);
        }

        return validationException;
    }

    public void setGrantType(String grantType) {
        this.grantType = grantType;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(SecureString password) {
        this.password = password;
    }

    public void setScope(@Nullable String scope) {
        this.scope = scope;
    }

    public String getGrantType() {
        return grantType;
    }

    public String getUsername() {
        return username;
    }

    public SecureString getPassword() {
        return password;
    }

    @Nullable
    public String getScope() {
        return scope;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(grantType);
        out.writeString(username);
        final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
        try {
            out.writeByteArray(passwordBytes);
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
        out.writeOptionalString(scope);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        grantType = in.readString();
        username = in.readString();
        final byte[] passwordBytes = in.readByteArray();
        try {
            password = new SecureString(CharArrays.utf8BytesToChars(passwordBytes));
        } finally {
            Arrays.fill(passwordBytes, (byte) 0);
        }
        scope = in.readOptionalString();
    }
}
