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

    // pkg scope for testing
    GetApiKeyRequest(@Nullable String realmName, @Nullable String userName, @Nullable String apiKeyId,
            @Nullable String apiKeyName) {
        if (Strings.hasText(realmName) == false && Strings.hasText(userName) == false && Strings.hasText(apiKeyId) == false
                && Strings.hasText(apiKeyName) == false) {
            throwValidationError("One of [api key id, api key name, username, realm name] must be specified");
        }
        if (Strings.hasText(apiKeyId) || Strings.hasText(apiKeyName)) {
            if (Strings.hasText(realmName) || Strings.hasText(userName)) {
                throwValidationError(
                        "username or realm name must not be specified when the api key id or api key name is specified");
            }
        }
        if (Strings.hasText(apiKeyId) && Strings.hasText(apiKeyName)) {
            throwValidationError("only one of [api key id, api key name] can be specified");
        }
        this.realmName = realmName;
        this.userName = userName;
        this.id = apiKeyId;
        this.name = apiKeyName;
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

    /**
     * Creates get API key request for given realm name
     * @param realmName realm name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingRealmName(String realmName) {
        return new GetApiKeyRequest(realmName, null, null, null);
    }

    /**
     * Creates get API key request for given user name
     * @param userName user name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingUserName(String userName) {
        return new GetApiKeyRequest(null, userName, null, null);
    }

    /**
     * Creates get API key request for given realm and user name
     * @param realmName realm name
     * @param userName user name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingRealmAndUserName(String realmName, String userName) {
        return new GetApiKeyRequest(realmName, userName, null, null);
    }

    /**
     * Creates get API key request for given api key id
     * @param apiKeyId api key id
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingApiKeyId(String apiKeyId) {
        return new GetApiKeyRequest(null, null, apiKeyId, null);
    }

    /**
     * Creates get API key request for given api key name
     * @param apiKeyName api key name
     * @return {@link GetApiKeyRequest}
     */
    public static GetApiKeyRequest usingApiKeyName(String apiKeyName) {
        return new GetApiKeyRequest(null, null, null, apiKeyName);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder;
    }

}
