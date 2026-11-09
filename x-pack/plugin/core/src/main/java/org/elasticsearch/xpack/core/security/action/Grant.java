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
import org.elasticsearch.xpack.core.security.authc.jwt.JwtRealmSettings;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Fields related to the end user authentication
 */
public class Grant implements Writeable {
    public static final String PASSWORD_GRANT_TYPE = "password";
    public static final String ACCESS_TOKEN_GRANT_TYPE = "access_token";
    /**
     * Grants on behalf of a user-managed service account by presenting one of its service account tokens. The credential
     * travels in its own {@code service_account_token} field rather than {@code access_token}: that field is reserved for
     * OAuth2 tokens and JWTs, and a dedicated grant type keeps the contract explicit, matching the grant of the same name
     * on the create token API. Tokens of built-in {@code elastic/*} accounts are refused by this grant.
     */
    public static final String USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE = "_user_managed_service_account";

    private String type;
    private String username;
    private SecureString password;
    private SecureString accessToken;
    private SecureString serviceAccountToken;
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
        this.runAsUsername = in.readOptionalString();
        this.clientAuthentication = in.readOptionalWriteable(ClientAuthentication::new);
        this.serviceAccountToken = in.readOptionalSecureString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(type);
        out.writeOptionalString(username);
        out.writeOptionalSecureString(password);
        out.writeOptionalSecureString(accessToken);
        out.writeOptionalString(runAsUsername);
        out.writeOptionalWriteable(clientAuthentication);
        out.writeOptionalSecureString(serviceAccountToken);
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

    public SecureString getServiceAccountToken() {
        return serviceAccountToken;
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

    public void setServiceAccountToken(SecureString serviceAccountToken) {
        this.serviceAccountToken = serviceAccountToken;
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
            validationException = validateUnsupportedField("service_account_token", serviceAccountToken, validationException);
            if (clientAuthentication != null) {
                return addValidationError("[client_authentication] is not supported for grant_type [" + type + "]", validationException);
            }
        } else if (type.equals(ACCESS_TOKEN_GRANT_TYPE)) {
            validationException = validateRequiredField("access_token", accessToken, validationException);
            validationException = validateUnsupportedField("username", username, validationException);
            validationException = validateUnsupportedField("password", password, validationException);
            validationException = validateUnsupportedField("service_account_token", serviceAccountToken, validationException);
            if (clientAuthentication != null
                && JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME.equals(clientAuthentication.scheme.trim()) == false) {
                return addValidationError(
                    "[client_authentication.scheme] must be set to [" + JwtRealmSettings.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + "]",
                    validationException
                );
            }
        } else if (type.equals(USER_MANAGED_SERVICE_ACCOUNT_GRANT_TYPE)) {
            validationException = validateRequiredField("service_account_token", serviceAccountToken, validationException);
            validationException = validateUnsupportedField("username", username, validationException);
            validationException = validateUnsupportedField("password", password, validationException);
            validationException = validateUnsupportedField("access_token", accessToken, validationException);
            // Service accounts cannot run-as, so rather than authenticating and then failing, refuse the request outright
            validationException = validateUnsupportedField("run_as", runAsUsername, validationException);
            if (clientAuthentication != null) {
                return addValidationError("[client_authentication] is not supported for grant_type [" + type + "]", validationException);
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
