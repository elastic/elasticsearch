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

/**
 * Request for get API key
 */
public final class GetApiKeyRequest implements Validatable, ToXContentObject {

    private final String realmName;
    private final String userName;
    private final String id;
    private final String name;
    private final boolean ownedByAuthenticatedUser;

    private GetApiKeyRequest() {
        this(null, null, null, null, false);
    }

    // pkg scope for testing
    GetApiKeyRequest(@Nullable String realmName, @Nullable String userName, @Nullable String apiKeyId,
                     @Nullable String apiKeyName, boolean ownedByAuthenticatedUser) {
        if (Strings.hasText(apiKeyId) || Strings.hasText(apiKeyName)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError(
                        "username or realm name must not be specified when the api key id or api key name is specified");
            }
        }
        if (ownedByAuthenticatedUser) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError("neither username nor realm-name may be specified when retrieving owned API keys");
            }
        }
        if (Strings.hasText(apiKeyId) && Strings.hasText(apiKeyName)) {
            throwValidationError("only one of [api key id, api key name] can be specified");
        }
        this.realmName = realmName;
        this.userName = userName;
        this.id = apiKeyId;
        this.name = apiKeyName;
        this.ownedByAuthenticatedUser = ownedByAuthenticatedUser;
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

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
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
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current
     * authenticated user else{@code false}
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingApiKeyId(String apiKeyId, boolean ownedByAuthenticatedUser) {
        return new GetApiKeyRequest(null, null, apiKeyId, null, ownedByAuthenticatedUser);
    }

    /**
     * Creates get API key request for given api key name
     * @param apiKeyName api key name
     * @param ownedByAuthenticatedUser set {@code true} if the request is only for the API keys owned by current
     * authenticated user else{@code false}
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
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

}
