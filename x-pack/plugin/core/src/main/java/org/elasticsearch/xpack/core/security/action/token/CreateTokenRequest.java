/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.CharArrays;

import java.io.IOException;
import java.util.Arrays;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Represents a request to create a token based on the provided information. This class accepts the
 * fields for an OAuth 2.0 access token request that uses the <code>password</code> grant type or the
 * <code>refresh_token</code> grant type.
 */
public final class CreateTokenRequest extends ActionRequest {

    private String grantType;
    private String username;
    private SecureString password;
    private String scope;
    private String refreshToken;

    public CreateTokenRequest() {}

    public CreateTokenRequest(String grantType, @Nullable String username, @Nullable SecureString password, @Nullable String scope,
                              @Nullable String refreshToken) {
        this.grantType = grantType;
        this.username = username;
        this.password = password;
        this.scope = scope;
        this.refreshToken = refreshToken;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if ("password".equals(grantType)) {
            if (Strings.isNullOrEmpty(username)) {
                validationException = addValidationError("username is missing", validationException);
            }
            if (password == null || password.getChars() == null || password.getChars().length == 0) {
                validationException = addValidationError("password is missing", validationException);
            }
            if (refreshToken != null) {
                validationException =
                        addValidationError("refresh_token is not supported with the password grant_type", validationException);
            }
        } else if ("refresh_token".equals(grantType)) {
            if (username != null) {
                validationException =
                        addValidationError("username is not supported with the refresh_token grant_type", validationException);
            }
            if (password != null) {
                validationException =
                        addValidationError("password is not supported with the refresh_token grant_type", validationException);
            }
            if (refreshToken == null) {
                validationException = addValidationError("refresh_token is missing", validationException);
            }
        } else {
            validationException = addValidationError("grant_type only supports the values: [password, refresh_token]", validationException);
        }

        return validationException;
    }

    public void setGrantType(String grantType) {
        this.grantType = grantType;
    }

    public void setUsername(@Nullable String username) {
        this.username = username;
    }

    public void setPassword(@Nullable SecureString password) {
        this.password = password;
    }

    public void setScope(@Nullable String scope) {
        this.scope = scope;
    }

    public void setRefreshToken(@Nullable String refreshToken) {
        this.refreshToken = refreshToken;
    }

    public String getGrantType() {
        return grantType;
    }

    @Nullable
    public String getUsername() {
        return username;
    }

    @Nullable
    public SecureString getPassword() {
        return password;
    }

    @Nullable
    public String getScope() {
        return scope;
    }

    @Nullable
    public String getRefreshToken() {
        return refreshToken;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(grantType);
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            out.writeOptionalString(username);
            if (password == null) {
                out.writeOptionalBytesReference(null);
            } else {
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                try {
                    out.writeOptionalBytesReference(new BytesArray(passwordBytes));
                } finally {
                    Arrays.fill(passwordBytes, (byte) 0);
                }
            }
            out.writeOptionalString(refreshToken);
        } else {
            if ("refresh_token".equals(grantType)) {
                throw new IllegalArgumentException("a refresh request cannot be sent to an older version");
            } else {
                out.writeString(username);
                final byte[] passwordBytes = CharArrays.toUtf8Bytes(password.getChars());
                try {
                    out.writeByteArray(passwordBytes);
                } finally {
                    Arrays.fill(passwordBytes, (byte) 0);
                }
            }
        }
        out.writeOptionalString(scope);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        grantType = in.readString();
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            username = in.readOptionalString();
            BytesReference bytesRef = in.readOptionalBytesReference();
            if (bytesRef != null) {
                byte[] bytes = BytesReference.toBytes(bytesRef);
                try {
                    password = new SecureString(CharArrays.utf8BytesToChars(bytes));
                } finally {
                    Arrays.fill(bytes, (byte) 0);
                }
            } else {
                password = null;
            }
            refreshToken = in.readOptionalString();
        } else {
            username = in.readString();
            final byte[] passwordBytes = in.readByteArray();
            try {
                password = new SecureString(CharArrays.utf8BytesToChars(passwordBytes));
            } finally {
                Arrays.fill(passwordBytes, (byte) 0);
            }
        }
        scope = in.readOptionalString();
    }
}
