/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Request for invalidating API key(s) so that it can no longer be used
 */
public final class InvalidateApiKeyRequest implements Validatable, ToXContentObject {

    private final String realmName;
    private final String userName;
    private final List<String> ids;
    private final String name;
    private final boolean ownedByAuthenticatedUser;

    InvalidateApiKeyRequest(
        @Nullable String realmName,
        @Nullable String userName,
        @Nullable String apiKeyName,
        boolean ownedByAuthenticatedUser,
        @Nullable List<String> apiKeyIds
    ) {
        validateApiKeyIds(apiKeyIds);
        if (Strings.hasText(realmName) == false
            && Strings.hasText(userName) == false
            && apiKeyIds == null
            && Strings.hasText(apiKeyName) == false
            && ownedByAuthenticatedUser == false) {
            throwValidationError("One of [api key id(s), api key name, username, realm name] must be specified if [owner] flag is false");
        }
        if (apiKeyIds != null || Strings.hasText(apiKeyName)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError("username or realm name must not be specified when the api key id(s) or api key name is specified");
            }
        }
        if (ownedByAuthenticatedUser) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError("neither username nor realm-name may be specified when invalidating owned API keys");
            }
        }
        if (apiKeyIds != null && Strings.hasText(apiKeyName)) {
            throwValidationError("only one of [api key id(s), api key name] can be specified");
        }
        this.realmName = realmName;
        this.userName = userName;
        this.ids = apiKeyIds == null ? null : List.copyOf(apiKeyIds);
        this.name = apiKeyName;
        this.ownedByAuthenticatedUser = ownedByAuthenticatedUser;
    }

    private void validateApiKeyIds(@Nullable List<String> apiKeyIds) {
        if (apiKeyIds != null) {
            if (apiKeyIds.size() == 0) {
                throwValidationError("Argument [apiKeyIds] cannot be an empty array");
            } else {
                final int[] idxOfBlankIds = IntStream.range(0, apiKeyIds.size())
                    .filter(i -> Strings.hasText(apiKeyIds.get(i)) == false)
                    .toArray();
                if (idxOfBlankIds.length > 0) {
                    throwValidationError(
                        "Argument [apiKeyIds] must not contain blank id, but got blank "
                            + (idxOfBlankIds.length == 1 ? "id" : "ids")
                            + " at index "
                            + (idxOfBlankIds.length == 1 ? "position" : "positions")
                            + ": "
                            + Arrays.toString(idxOfBlankIds)
                    );
                }
            }
        }
    }

    private void throwValidationError(String message) {
        throw new IllegalArgumentException(message);
    }

    public String getRealmName() {
        return realmName;
    }

    public String getUserName() {
        return userName;
    }

    @Deprecated
    public String getId() {
        if (ids == null) {
            return null;
        } else if (ids.size() == 1) {
            return ids.get(0);
        } else {
            throw new IllegalArgumentException(
                "Cannot get a single api key id when multiple ids have been set [" + Strings.collectionToCommaDelimitedString(ids) + "]"
            );
        }
    }

    public List<String> getIds() {
        return ids;
    }

    public String getName() {
        return name;
    }

    public boolean ownedByAuthenticatedUser() {
        return ownedByAuthenticatedUser;
    }

    /**
     * Creates invalidate API key request for given realm name
     * @param realmName realm name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmName(String realmName) {
        return new InvalidateApiKeyRequest(realmName, null, null, false, null);
    }

    /**
     * Creates invalidate API key request for given user name
     * @param userName user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingUserName(String userName) {
        return new InvalidateApiKeyRequest(null, userName, null, false, null);
    }

    /**
     * Creates invalidate API key request for given realm and user name
     * @param realmName realm name
     * @param userName user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmAndUserName(String realmName, String userName) {
        return new InvalidateApiKeyRequest(realmName, userName, null, false, null);
    }

    /**
     * Creates invalidate API key request for given api key id
     * @param apiKeyId api key id
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyId(String apiKeyId, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, ownedByAuthenticatedUser, apiKeyIdToIds(apiKeyId));
    }

    /**
     * Creates invalidate API key request for given api key ids
     * @param apiKeyIds api key ids
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyIds(List<String> apiKeyIds, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, ownedByAuthenticatedUser, apiKeyIds);
    }

    /**
     * Creates invalidate API key request for given api key name
     * @param apiKeyName api key name
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyName(String apiKeyName, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, apiKeyName, ownedByAuthenticatedUser, null);
    }

    /**
     * Creates invalidate api key request to invalidate api keys owned by the current authenticated user.
     */
    public static InvalidateApiKeyRequest forOwnedApiKeys() {
        return new InvalidateApiKeyRequest(null, null, null, true, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (realmName != null) {
            builder.field("realm_name", realmName);
        }
        if (userName != null) {
            builder.field("username", userName);
        }
        if (ids != null) {
            builder.field("ids", ids);
        }
        if (name != null) {
            builder.field("name", name);
        }
        builder.field("owner", ownedByAuthenticatedUser);
        return builder.endObject();
    }

    static List<String> apiKeyIdToIds(@Nullable String apiKeyId) {
        return Strings.hasText(apiKeyId) ? List.of(apiKeyId) : null;
    }
}
