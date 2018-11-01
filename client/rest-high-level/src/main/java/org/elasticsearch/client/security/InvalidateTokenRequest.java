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
import java.util.Objects;

/**
 * Request to invalidate a OAuth2 token within the Elasticsearch cluster.
 */
public final class InvalidateTokenRequest implements Validatable, ToXContentObject {

    private final String accessToken;
    private final String refreshToken;

    InvalidateTokenRequest(@Nullable String accessToken, @Nullable String refreshToken) {
        if (Strings.isNullOrEmpty(accessToken)) {
            if (Strings.isNullOrEmpty(refreshToken)) {
                throw new IllegalArgumentException("Either access-token or refresh-token is required");
            }
        } else if (Strings.isNullOrEmpty(refreshToken) == false) {
            throw new IllegalArgumentException("Cannot supply both access-token and refresh-token");
        }
        this.accessToken = accessToken;
        this.refreshToken = refreshToken;
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

    public String getAccessToken() {
        return accessToken;
    }

    public String getRefreshToken() {
        return refreshToken;
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
        final InvalidateTokenRequest that = (InvalidateTokenRequest) o;
        return Objects.equals(this.accessToken, that.accessToken) &&
            Objects.equals(this.refreshToken, that.refreshToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, refreshToken);
    }
}
