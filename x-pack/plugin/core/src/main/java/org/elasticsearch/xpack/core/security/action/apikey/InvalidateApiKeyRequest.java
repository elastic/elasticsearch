/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * Request for invalidating API key(s) so that it can no longer be used
 */
public final class InvalidateApiKeyRequest extends ActionRequest {

    private final String realmName;
    private final String userName;
    private final String[] ids;
    private final String name;
    private final boolean ownedByAuthenticatedUser;

    public InvalidateApiKeyRequest() {
        this(null, null, null, false, null);
    }

    public InvalidateApiKeyRequest(StreamInput in) throws IOException {
        super(in);
        realmName = textOrNull(in.readOptionalString());
        userName = textOrNull(in.readOptionalString());
        ids = in.readOptionalStringArray();
        validateIds(ids);
        name = textOrNull(in.readOptionalString());
        ownedByAuthenticatedUser = in.readOptionalBoolean();
    }

    public InvalidateApiKeyRequest(
        @Nullable String realmName,
        @Nullable String userName,
        @Nullable String name,
        boolean ownedByAuthenticatedUser,
        @Nullable String[] ids
    ) {
        validateIds(ids);
        this.realmName = textOrNull(realmName);
        this.userName = textOrNull(userName);
        this.ids = ids;
        this.name = textOrNull(name);
        this.ownedByAuthenticatedUser = ownedByAuthenticatedUser;
    }

    private static String textOrNull(@Nullable String arg) {
        return Strings.hasText(arg) ? arg : null;
    }

    public String getRealmName() {
        return realmName;
    }

    public String getUserName() {
        return userName;
    }

    public String[] getIds() {
        return ids;
    }

    public String getName() {
        return name;
    }

    public boolean ownedByAuthenticatedUser() {
        return ownedByAuthenticatedUser;
    }

    /**
     * Creates invalidate api key request for given realm name
     *
     * @param realmName realm name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmName(String realmName) {
        return new InvalidateApiKeyRequest(realmName, null, null, false, null);
    }

    /**
     * Creates invalidate API key request for given user name
     *
     * @param userName user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingUserName(String userName) {
        return new InvalidateApiKeyRequest(null, userName, null, false, null);
    }

    /**
     * Creates invalidate API key request for given realm and user name
     *
     * @param realmName realm name
     * @param userName  user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmAndUserName(String realmName, String userName) {
        return new InvalidateApiKeyRequest(realmName, userName, null, false, null);
    }

    /**
     * Creates invalidate API key request for given api key ids
     *
     * @param id api key id
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyId(String id, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, ownedByAuthenticatedUser, new String[] { id });
    }

    /**
     * Creates invalidate API key request for given api key id
     *
     * @param ids array of api key ids
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyIds(String[] ids, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, ownedByAuthenticatedUser, ids);
    }

    /**
     * Creates invalidate api key request for given api key name
     *
     * @param name api key name
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyName(String name, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, name, ownedByAuthenticatedUser, null);
    }

    /**
     * Creates invalidate api key request to invalidate api keys owned by the current authenticated user.
     */
    public static InvalidateApiKeyRequest forOwnedApiKeys() {
        return new InvalidateApiKeyRequest(null, null, null, true, null);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (Strings.hasText(realmName) == false
            && Strings.hasText(userName) == false
            && ids == null
            && Strings.hasText(name) == false
            && ownedByAuthenticatedUser == false) {
            validationException = addValidationError(
                "One of [api key id(s), api key name, username, realm name] must be specified if " + "[owner] flag is false",
                validationException
            );
        }
        if (ids != null || Strings.hasText(name)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                validationException = addValidationError(
                    "username or realm name must not be specified when the api key id(s) or api key name are specified",
                    validationException
                );
            }
        }
        if (ownedByAuthenticatedUser) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                validationException = addValidationError(
                    "neither username nor realm-name may be specified when invalidating owned API keys",
                    validationException
                );
            }
        }
        if (ids != null && Strings.hasText(name)) {
            validationException = addValidationError("only one of [api key id(s), api key name] can be specified", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(realmName);
        out.writeOptionalString(userName);
        out.writeOptionalStringArray(ids);
        out.writeOptionalString(name);
        out.writeOptionalBoolean(ownedByAuthenticatedUser);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        InvalidateApiKeyRequest that = (InvalidateApiKeyRequest) o;
        return ownedByAuthenticatedUser == that.ownedByAuthenticatedUser
            && Objects.equals(realmName, that.realmName)
            && Objects.equals(userName, that.userName)
            && Arrays.equals(ids, that.ids)
            && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(realmName, userName, Arrays.hashCode(ids), name, ownedByAuthenticatedUser);
    }

    private static void validateIds(@Nullable String[] idsToValidate) {
        if (idsToValidate != null) {
            if (idsToValidate.length == 0) {
                final ActionRequestValidationException validationException = new ActionRequestValidationException();
                validationException.addValidationError("Field [ids] cannot be an empty array");
                throw validationException;
            } else {
                final int[] idxOfBlankIds = IntStream.range(0, idsToValidate.length)
                    .filter(i -> Strings.hasText(idsToValidate[i]) == false)
                    .toArray();
                if (idxOfBlankIds.length > 0) {
                    final ActionRequestValidationException validationException = new ActionRequestValidationException();
                    validationException.addValidationError(
                        "Field [ids] must not contain blank id, but got blank "
                            + (idxOfBlankIds.length == 1 ? "id" : "ids")
                            + " at index "
                            + (idxOfBlankIds.length == 1 ? "position" : "positions")
                            + ": "
                            + Arrays.toString(idxOfBlankIds)
                    );
                    throw validationException;
                }
            }
        }
    }
}
