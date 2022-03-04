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
import java.util.Objects;

/**
 * Request to invalidate a OAuth2 token within the Elasticsearch cluster.
 */
public final class InvalidateTokenRequest implements Validatable, ToXContentObject {

    private final String accessToken;
    private final String refreshToken;
    private final String realmName;
    private final String username;

    InvalidateTokenRequest(@Nullable String accessToken, @Nullable String refreshToken) {
        this(accessToken, refreshToken, null, null);
    }

    public InvalidateTokenRequest(
        @Nullable String accessToken,
        @Nullable String refreshToken,
        @Nullable String realmName,
        @Nullable String username
    ) {
        if (Strings.hasText(realmName) || Strings.hasText(username)) {
            if (Strings.hasText(accessToken)) {
                throw new IllegalArgumentException("access token is not allowed when realm name or username are specified");
            }
            if (refreshToken != null) {
                throw new IllegalArgumentException("refresh token is not allowed when realm name or username are specified");
            }
        } else {
            if (Strings.isNullOrEmpty(accessToken)) {
                if (Strings.isNullOrEmpty(refreshToken)) {
                    throw new IllegalArgumentException(
                        "Either access token or refresh token is required when neither realm name or " + "username are specified"
                    );
                }
            } else if (Strings.isNullOrEmpty(refreshToken) == false) {
                throw new IllegalArgumentException("Cannot supply both access token and refresh token");
            }
        }
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
        this.realmName = realmName;
        this.username = username;
    }

    public static InvalidateTokenRequest accessToken(String accessToken) {
        if (Strings.isNullOrEmpty(accessToken)) {
            throw new IllegalArgumentException("token is required");
        }
        return new InvalidateTokenRequest(accessToken, null);
    }

    public static InvalidateTokenRequest refreshToken(String refreshToken) {
        if (Strings.isNullOrEmpty(refreshToken)) {
            throw new IllegalArgumentException("refresh_token is required");
        }
        return new InvalidateTokenRequest(null, refreshToken);
    }

    public static InvalidateTokenRequest realmTokens(String realmName) {
        if (Strings.isNullOrEmpty(realmName)) {
            throw new IllegalArgumentException("realm name is required");
        }
        return new InvalidateTokenRequest(null, null, realmName, null);
    }

    public static InvalidateTokenRequest userTokens(String username) {
        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalArgumentException("username is required");
        }
        return new InvalidateTokenRequest(null, null, null, username);
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getRealmName() {
        return realmName;
    }

    public String getUsername() {
        return username;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (accessToken != null) {
            builder.field("token", accessToken);
        }
        if (refreshToken != null) {
            builder.field("refresh_token", refreshToken);
        }
        if (realmName != null) {
            builder.field("realm_name", realmName);
        }
        if (username != null) {
            builder.field("username", username);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InvalidateTokenRequest that = (InvalidateTokenRequest) o;
        return Objects.equals(accessToken, that.accessToken)
            && Objects.equals(refreshToken, that.refreshToken)
            && Objects.equals(realmName, that.realmName)
            && Objects.equals(username, that.username);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, refreshToken, realmName, username);
    }
}
