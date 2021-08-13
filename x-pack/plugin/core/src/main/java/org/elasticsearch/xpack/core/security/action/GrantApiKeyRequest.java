/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request class used for the creation of an API key on behalf of another user.
 * Logically this is similar to {@link CreateApiKeyRequest}, but is for cases when the user that has permission to call this action
 * is different to the user for whom the API key should be created
 */
public final class GrantApiKeyRequest extends ActionRequest {

    public static final String PASSWORD_GRANT_TYPE = "password";
    public static final String ACCESS_TOKEN_GRANT_TYPE = "access_token";

    /**
     * Fields related to the end user authentication
     */
    public static class Grant implements Writeable {
        private String type;
        private String username;
        private SecureString password;
        private SecureString accessToken;

        public Grant() {
        }

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
    }

    private final Grant grant;
    private CreateApiKeyRequest apiKey;

    public GrantApiKeyRequest() {
        this.grant = new Grant();
        this.apiKey = new CreateApiKeyRequest();
    }

    public GrantApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        this.grant = new Grant(in);
        this.apiKey = new CreateApiKeyRequest(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        grant.writeTo(out);
        apiKey.writeTo(out);
    }

    public WriteRequest.RefreshPolicy getRefreshPolicy() {
        return apiKey.getRefreshPolicy();
    }

    public void setRefreshPolicy(WriteRequest.RefreshPolicy refreshPolicy) {
        apiKey.setRefreshPolicy(refreshPolicy);
    }

    public Grant getGrant() {
        return grant;
    }

    public CreateApiKeyRequest getApiKeyRequest() {
        return apiKey;
    }

    public void setApiKeyRequest(CreateApiKeyRequest apiKeyRequest) {
        this.apiKey = Objects.requireNonNull(apiKeyRequest, "Cannot set a null api_key");
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = apiKey.validate();
        if (grant.type == null) {
            validationException = addValidationError("[grant_type] is required", validationException);
        } else if (grant.type.equals(PASSWORD_GRANT_TYPE)) {
            validationException = validateRequiredField("username", grant.username, validationException);
            validationException = validateRequiredField("password", grant.password, validationException);
            validationException = validateUnsupportedField("access_token", grant.accessToken, validationException);
        } else if (grant.type.equals(ACCESS_TOKEN_GRANT_TYPE)) {
            validationException = validateRequiredField("access_token", grant.accessToken, validationException);
            validationException = validateUnsupportedField("username", grant.username, validationException);
            validationException = validateUnsupportedField("password", grant.password, validationException);
        } else {
            validationException = addValidationError("grant_type [" + grant.type + "] is not supported", validationException);
        }
        return validationException;
    }

    private ActionRequestValidationException validateRequiredField(String fieldName, CharSequence fieldValue,
                                                                   ActionRequestValidationException validationException) {
        if (fieldValue == null || fieldValue.length() == 0) {
            return addValidationError("[" + fieldName + "] is required for grant_type [" + grant.type + "]", validationException);
        }
        return validationException;
    }

    private ActionRequestValidationException validateUnsupportedField(String fieldName, CharSequence fieldValue,
                                                                      ActionRequestValidationException validationException) {
        if (fieldValue != null && fieldValue.length() > 0) {
            return addValidationError("[" + fieldName + "] is not supported for grant_type [" + grant.type + "]", validationException);
        }
        return validationException;
    }
}
