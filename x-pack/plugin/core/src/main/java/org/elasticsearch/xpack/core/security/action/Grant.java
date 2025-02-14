/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

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
    private String runAsUsername;
    private ClientAuthentication clientAuthentication;

    public record ClientAuthentication(String scheme, SecureString value) implements Writeable {

        public ClientAuthentication(SecureString value) {
            this(JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME, value);
        }

        ClientAuthentication(StreamInput in) throws IOException {
            this(in.readString(), in.readSecureString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(scheme);
            out.writeSecureString(value);
        }
    }

    public Grant() {}

    public Grant(StreamInput in) throws IOException {
        this.type = in.readString();
        this.username = in.readOptionalString();
        this.password = in.readOptionalSecureString();
        this.accessToken = in.readOptionalSecureString();
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            this.runAsUsername = in.readOptionalString();
        } else {
            this.runAsUsername = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.clientAuthentication = in.readOptionalWriteable(ClientAuthentication::new);
        } else {
            this.clientAuthentication = null;
        }
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(username);
        out.writeOptionalSecureString(password);
        out.writeOptionalSecureString(accessToken);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_4_0)) {
            out.writeOptionalString(runAsUsername);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeOptionalWriteable(clientAuthentication);
        }
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

    public String getRunAsUsername() {
        return runAsUsername;
    }

    public ClientAuthentication getClientAuthentication() {
        return clientAuthentication;
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

    public void setRunAsUsername(String runAsUsername) {
        this.runAsUsername = runAsUsername;
    }

    public void setClientAuthentication(ClientAuthentication clientAuthentication) {
        this.clientAuthentication = clientAuthentication;
    }

    public ActionRequestValidationException validate(ActionRequestValidationException validationException) {
        if (type == null) {
            validationException = addValidationError("[grant_type] is required", validationException);
        } else if (type.equals(PASSWORD_GRANT_TYPE)) {
            validationException = validateRequiredField("username", username, validationException);
            validationException = validateRequiredField("password", password, validationException);
            validationException = validateUnsupportedField("access_token", accessToken, validationException);
            if (clientAuthentication != null) {
                return addValidationError("[client_authentication] is not supported for grant_type [" + type + "]", validationException);
            }
        } else if (type.equals(ACCESS_TOKEN_GRANT_TYPE)) {
            validationException = validateRequiredField("access_token", accessToken, validationException);
            validationException = validateUnsupportedField("username", username, validationException);
            validationException = validateUnsupportedField("password", password, validationException);
            if (clientAuthentication != null
                && JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME.equals(clientAuthentication.scheme.trim()) == false) {
                return addValidationError(
                    "[client_authentication.scheme] must be set to [" + JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + "]",
                    validationException
                );
            }
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
