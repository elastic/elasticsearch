/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Request for invalidating API key(s) so that it can no longer be used
 */
public final class InvalidateApiKeyRequest implements Validatable, ToXContentObject {

    private final String realmName;
    private final String userName;
    @Deprecated
    private final String id;
    private final String name;
    private final boolean ownedByAuthenticatedUser;
    @Nullable
    private final String[] ids;

    // pkg scope for testing
    InvalidateApiKeyRequest(@Nullable String realmName, @Nullable String userName, @Nullable String apiKeyId,
                            @Nullable String apiKeyName, boolean ownedByAuthenticatedUser) {
        this(realmName, userName, apiKeyId, apiKeyName, ownedByAuthenticatedUser, null);
    }

    InvalidateApiKeyRequest(@Nullable String realmName, @Nullable String userName, @Nullable String apiKeyId,
                            @Nullable String apiKeyName, boolean ownedByAuthenticatedUser, @Nullable String[] ids) {
        if (Strings.hasText(realmName) == false && Strings.hasText(userName) == false && Strings.hasText(apiKeyId) == false
                && Strings.hasText(apiKeyName) == false && ownedByAuthenticatedUser == false && hasNonBlankId(ids) == false) {
            throwValidationError("One of [api key ids, api key name, username, realm name] must be specified if [owner] flag is false");
        }
        if (Strings.hasText(apiKeyId) || Strings.hasText(apiKeyName) || hasNonBlankId(ids)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError(
                        "username or realm name must not be specified when the api key ids or api key name is specified");
            }
        }
        if (ownedByAuthenticatedUser) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError("neither username nor realm-name may be specified when invalidating owned API keys");
            }
        }
        if ((Strings.hasText(apiKeyId) || hasNonBlankId(ids)) && Strings.hasText(apiKeyName)) {
            throwValidationError("only one of [api key ids, api key name] can be specified");
        }
        this.realmName = realmName;
        this.userName = userName;
        this.id = apiKeyId;
        this.name = apiKeyName;
        this.ownedByAuthenticatedUser = ownedByAuthenticatedUser;
        this.ids = ids;
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
        return id;
    }

    public String getName() {
        return name;
    }

    public boolean ownedByAuthenticatedUser() {
        return ownedByAuthenticatedUser;
    }

    public Set<String> getAllIds() {
        Set<String> apiKeyIds = new HashSet<>();
        if (Strings.hasText(id)) {
            apiKeyIds.add(id);
        }
        if (ids != null) {
            apiKeyIds.addAll(Arrays.stream(ids).filter(Strings::hasText).collect(Collectors.toList()));
        }
        return Set.copyOf(apiKeyIds);
    }

    /**
     * Creates invalidate API key request for given realm name
     * @param realmName realm name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmName(String realmName) {
        return new InvalidateApiKeyRequest(realmName, null, null, null, false);
    }

    /**
     * Creates invalidate API key request for given user name
     * @param userName user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingUserName(String userName) {
        return new InvalidateApiKeyRequest(null, userName, null, null, false);
    }

    /**
     * Creates invalidate API key request for given realm and user name
     * @param realmName realm name
     * @param userName user name
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingRealmAndUserName(String realmName, String userName) {
        return new InvalidateApiKeyRequest(realmName, userName, null, null, false);
    }

    /**
     * Creates invalidate API key request for given api key id
     * @param apiKeyIds api key id
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyIds(String[] apiKeyIds, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, null, ownedByAuthenticatedUser, apiKeyIds);
    }

    /**
     * Creates invalidate API key request for given api key name
     * @param apiKeyName api key name
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current authenticated user else
     * {@code false}
     * @return {@link InvalidateApiKeyRequest}
     */
    public static InvalidateApiKeyRequest usingApiKeyName(String apiKeyName, boolean ownedByAuthenticatedUser) {
        return new InvalidateApiKeyRequest(null, null, null, apiKeyName, ownedByAuthenticatedUser);
    }

    /**
     * Creates invalidate api key request to invalidate api keys owned by the current authenticated user.
     */
    public static InvalidateApiKeyRequest forOwnedApiKeys() {
        return new InvalidateApiKeyRequest(null, null, null, null, true);
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
        if (name != null) {
            builder.field("name", name);
        }
        final Set<String> allIds = getAllIds();
        if (allIds.isEmpty() == false) {
            builder.array("ids", allIds.toArray(String[]::new));
        }
        builder.field("owner", ownedByAuthenticatedUser);
        return builder.endObject();
    }

    private static boolean hasNonBlankId(String[] ids) {
        if (ids == null || ids.length == 0) {
            return false;
        }
        return Arrays.stream(ids).anyMatch(Strings::hasText);
    }
}
