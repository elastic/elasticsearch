/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for get API key
 */
public final class GetApiKeyRequest extends ActionRequest {

    private final String realmName;
    private final String userName;
    private final String apiKeyId;
    private final String apiKeyName;
    private final boolean ownedByAuthenticatedUser;

    public GetApiKeyRequest() {
        this(null, null, null, null, false);
    }

    public GetApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        realmName = in.readOptionalString();
        userName = in.readOptionalString();
        apiKeyId = in.readOptionalString();
        apiKeyName = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_4_0)) {
            ownedByAuthenticatedUser = in.readOptionalBoolean();
        } else {
            ownedByAuthenticatedUser = false;
        }
    }

    public GetApiKeyRequest(@Nullable String realmName, @Nullable String userName, @Nullable String apiKeyId,
                            @Nullable String apiKeyName, boolean ownedByAuthenticatedUser) {
        this.realmName = realmName;
        this.userName = userName;
        this.apiKeyId = apiKeyId;
        this.apiKeyName = apiKeyName;
        this.ownedByAuthenticatedUser = ownedByAuthenticatedUser;
    }

    public String getRealmName() {
        return realmName;
    }

    public String getUserName() {
        return userName;
    }

    public String getApiKeyId() {
        return apiKeyId;
    }

    public String getApiKeyName() {
        return apiKeyName;
    }

    public boolean ownedByAuthenticatedUser() {
        return ownedByAuthenticatedUser;
    }

    /**
     * Creates get API key request for given realm name
     * @param realmName realm name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingRealmName(String realmName) {
        return new GetApiKeyRequest(realmName, null, null, null, false);
    }

    /**
     * Creates get API key request for given user name
     * @param userName user name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingUserName(String userName) {
        return new GetApiKeyRequest(null, userName, null, null, false);
    }

    /**
     * Creates get API key request for given realm and user name
     * @param realmName realm name
     * @param userName user name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingRealmAndUserName(String realmName, String userName) {
        return new GetApiKeyRequest(realmName, userName, null, null, false);
    }

    /**
     * Creates get API key request for given api key id
     * @param apiKeyId api key id
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingApiKeyId(String apiKeyId, boolean ownedByAuthenticatedUser) {
        return new GetApiKeyRequest(null, null, apiKeyId, null, ownedByAuthenticatedUser);
    }

    /**
     * Creates get api key request for given api key name
     * @param apiKeyName api key name
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingApiKeyName(String apiKeyName, boolean ownedByAuthenticatedUser) {
        return new GetApiKeyRequest(null, null, null, apiKeyName, ownedByAuthenticatedUser);
    }

    /**
     * Creates get api key request to retrieve api key information for the api keys owned by the current authenticated user.
     */
    public static GetApiKeyRequest forOwnedApiKeys() {
        return new GetApiKeyRequest(null, null, null, null, true);
    }

    /**
     * Creates get api key request to retrieve api key information for all api keys if the authenticated user is authorized to do so.
     */
    public static GetApiKeyRequest forAllApiKeys() {
        return new GetApiKeyRequest();
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(apiKeyId) || Strings.hasText(apiKeyName)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                validationException = addValidationError(
                        "username or realm name must not be specified when the api key id or api key name is specified",
                        validationException);
            }
        }
        if (ownedByAuthenticatedUser) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                validationException = addValidationError(
                    "neither username nor realm-name may be specified when retrieving owned API keys",
                    validationException);
            }
        }
        if (Strings.hasText(apiKeyId) && Strings.hasText(apiKeyName)) {
            validationException = addValidationError("only one of [api key id, api key name] can be specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(realmName);
        out.writeOptionalString(userName);
        out.writeOptionalString(apiKeyId);
        out.writeOptionalString(apiKeyName);
        if (out.getVersion().onOrAfter(Version.V_7_4_0)) {
            out.writeOptionalBoolean(ownedByAuthenticatedUser);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetApiKeyRequest that = (GetApiKeyRequest) o;
        return ownedByAuthenticatedUser == that.ownedByAuthenticatedUser &&
            Objects.equals(realmName, that.realmName) &&
            Objects.equals(userName, that.userName) &&
            Objects.equals(apiKeyId, that.apiKeyId) &&
            Objects.equals(apiKeyName, that.apiKeyName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(realmName, userName, apiKeyId, apiKeyName, ownedByAuthenticatedUser);
    }
}
