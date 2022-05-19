/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.support.BearerToken;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Fields related to the end user authentication
 */
public class Grant implements Writeable {
    public static final String PASSWORD_GRANT_TYPE = "password";
    public static final String ACCESS_TOKEN_GRANT_TYPE = "access_token";

    private String type;
    private String username;
    private SecureString password;
    private SecureString accessToken;

    public Grant() {}

    public Grant(StreamInput in) throws IOException {
        this.type = in.readString();
        this.username = in.readOptionalString();
        this.password = in.readOptionalSecureString();
        this.accessToken = in.readOptionalSecureString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(username);
        out.writeOptionalSecureString(password);
        out.writeOptionalSecureString(accessToken);
    }

    public String getType() {
        return type;
    }

    public String getUsername() {
        return username;
    }

    public SecureString getPassword() {
        return password;
    }

    public SecureString getAccessToken() {
        return accessToken;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public void setPassword(SecureString password) {
        this.password = password;
    }

    public void setAccessToken(SecureString accessToken) {
        this.accessToken = accessToken;
    }

    public AuthenticationToken getAuthenticationToken() {
        assert validate(null) == null : "grant is invalid";
        return switch (type) {
            case PASSWORD_GRANT_TYPE -> new UsernamePasswordToken(username, password);
            case ACCESS_TOKEN_GRANT_TYPE -> new BearerToken(accessToken);
            default -> null;
        };
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (type == null) {
            validationException = addValidationError("[grant_type] is required", validationException);
        } else if (type.equals(PASSWORD_GRANT_TYPE)) {
            validationException = validateRequiredField("username", username, validationException);
            validationException = validateRequiredField("password", password, validationException);
            validationException = validateUnsupportedField("access_token", accessToken, validationException);
        } else if (type.equals(ACCESS_TOKEN_GRANT_TYPE)) {
            validationException = validateRequiredField("access_token", accessToken, validationException);
            validationException = validateUnsupportedField("username", username, validationException);
            validationException = validateUnsupportedField("password", password, validationException);
        } else {
            validationException = addValidationError("grant_type [" + type + "] is not supported", validationException);
        }
        return validationException;
    }

    private ActionRequestValidationException validateRequiredField(
        String fieldName,
        CharSequence fieldValue,
        ActionRequestValidationException validationException
    ) {
        if (fieldValue == null || fieldValue.length() == 0) {
            return addValidationError("[" + fieldName + "] is required for grant_type [" + type + "]", validationException);
        }
        return validationException;
    }

    private ActionRequestValidationException validateUnsupportedField(
        String fieldName,
        CharSequence fieldValue,
        ActionRequestValidationException validationException
    ) {
        if (fieldValue != null && fieldValue.length() > 0) {
            return addValidationError("[" + fieldName + "] is not supported for grant_type [" + type + "]", validationException);
        }
        return validationException;
    }
}
