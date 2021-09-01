/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.Validatable;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request to create a new API Key on behalf of another user.
 */
public final class GrantApiKeyRequest implements Validatable, ToXContentObject {

    private final Grant grant;
    private final CreateApiKeyRequest apiKeyRequest;

    public static class Grant implements ToXContentFragment {
        private final String grantType;
        private final String username;
        private final char[] password;
        private final String accessToken;

        private Grant(String grantType, String username, char[] password, String accessToken) {
            this.grantType = Objects.requireNonNull(grantType, "Grant type may not be null");
            this.username = username;
            this.password = password;
            this.accessToken = accessToken;
        }

        public static Grant passwordGrant(String username, char[] password) {
            return new Grant(
                "password",
                Objects.requireNonNull(username, "Username may not be null"),
                Objects.requireNonNull(password, "Password may not be null"),
                null);
        }

        public static Grant accessTokenGrant(String accessToken) {
            return new Grant(
                "access_token",
                null,
                null,
                Objects.requireNonNull(accessToken, "Access token may not be null")
            );
        }

        public String getGrantType() {
            return grantType;
        }

        public String getUsername() {
            return username;
        }

        public char[] getPassword() {
            return password;
        }

        public String getAccessToken() {
            return accessToken;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field("grant_type", grantType);
            if (username != null) {
                builder.field("username", username);
            }
            if (password != null) {
                byte[] passwordBytes = CharArrays.toUtf8Bytes(password);
                try {
                    builder.field("password").utf8Value(passwordBytes, 0, passwordBytes.length);
                } finally {
                    Arrays.fill(passwordBytes, (byte) 0);
                }
            }
            if (accessToken != null) {
                builder.field("access_token", accessToken);
            }
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Grant grant = (Grant) o;
            return grantType.equals(grant.grantType)
                && Objects.equals(username, grant.username)
                && Arrays.equals(password, grant.password)
                && Objects.equals(accessToken, grant.accessToken);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(grantType, username, accessToken);
            result = 31 * result + Arrays.hashCode(password);
            return result;
        }
    }

    public GrantApiKeyRequest(Grant grant, CreateApiKeyRequest apiKeyRequest) {
        this.grant = Objects.requireNonNull(grant, "Grant may not be null");
        this.apiKeyRequest = Objects.requireNonNull(apiKeyRequest, "Create API key request may not be null");
    }

    public Grant getGrant() {
        return grant;
    }

    public CreateApiKeyRequest getApiKeyRequest() {
        return apiKeyRequest;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        grant.toXContent(builder, params);
        builder.field("api_key", apiKeyRequest);
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final GrantApiKeyRequest that = (GrantApiKeyRequest) o;
        return Objects.equals(this.grant, that.grant)
            && Objects.equals(this.apiKeyRequest, that.apiKeyRequest);
    }

    @Override
    public int hashCode() {
        return Objects.hash(grant, apiKeyRequest);
    }
}
