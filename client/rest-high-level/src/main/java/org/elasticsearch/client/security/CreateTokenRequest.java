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
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Request to create a new OAuth2 token from the Elasticsearch cluster.
 */
public final class CreateTokenRequest implements Validatable, ToXContentObject {

    private final String grantType;
    private final String scope;
    private final String username;
    private final char[] password;
    private final String refreshToken;
    private final char[] kerberosTicket;

    /**
     * General purpose constructor. This constructor is typically not useful, and one of the following factory methods should be used
     * instead:
     * <ul>
     * <li>{@link #passwordGrant(String, char[])}</li>
     * <li>{@link #refreshTokenGrant(String)}</li>
     * <li>{@link #clientCredentialsGrant()}</li>
     * <li>{@link #kerberosGrant(char[])}</li>
     * </ul>
     */
    public CreateTokenRequest(String grantType, @Nullable String scope, @Nullable String username, @Nullable char[] password,
                              @Nullable String refreshToken, @Nullable char[] kerberosTicket) {
        if (Strings.isNullOrEmpty(grantType)) {
            throw new IllegalArgumentException("grant_type is required");
        }
        this.grantType = grantType;
        this.username = username;
        this.password = password;
        this.scope = scope;
        this.refreshToken = refreshToken;
        this.kerberosTicket = kerberosTicket;
    }

    public static CreateTokenRequest passwordGrant(String username, char[] password) {
        if (Strings.isNullOrEmpty(username)) {
            throw new IllegalArgumentException("username is required");
        }
        if (password == null || password.length == 0) {
            throw new IllegalArgumentException("password is required");
        }
        return new CreateTokenRequest("password", null, username, password, null, null);
    }

    public static CreateTokenRequest refreshTokenGrant(String refreshToken) {
        if (Strings.isNullOrEmpty(refreshToken)) {
            throw new IllegalArgumentException("refresh_token is required");
        }
        return new CreateTokenRequest("refresh_token", null, null, null, refreshToken, null);
    }

    public static CreateTokenRequest clientCredentialsGrant() {
        return new CreateTokenRequest("client_credentials", null, null, null, null, null);
    }

    public static CreateTokenRequest kerberosGrant(char[] kerberosTicket) {
        if (kerberosTicket == null || kerberosTicket.length == 0) {
            throw new IllegalArgumentException("kerberos ticket is required");
        }
        return new CreateTokenRequest("_kerberos", null, null, null, null, kerberosTicket);
    }

    public String getGrantType() {
        return grantType;
    }

    public String getScope() {
        return scope;
    }

    public String getUsername() {
        return username;
    }

    public char[] getPassword() {
        return password;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public char[] getKerberosTicket() {
        return kerberosTicket;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("grant_type", grantType);
        if (scope != null) {
            builder.field("scope", scope);
        }
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
        if (refreshToken != null) {
            builder.field("refresh_token", refreshToken);
        }
        if (kerberosTicket != null) {
            byte[] kerberosTicketBytes = CharArrays.toUtf8Bytes(kerberosTicket);
            try {
                builder.field("kerberos_ticket").utf8Value(kerberosTicketBytes, 0, kerberosTicketBytes.length);
            } finally {
                Arrays.fill(kerberosTicketBytes, (byte) 0);
            }
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
        final CreateTokenRequest that = (CreateTokenRequest) o;
        return Objects.equals(grantType, that.grantType) &&
            Objects.equals(scope, that.scope) &&
            Objects.equals(username, that.username) &&
            Arrays.equals(password, that.password) &&
            Objects.equals(refreshToken, that.refreshToken) &&
            Arrays.equals(kerberosTicket, that.kerberosTicket);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(grantType, scope, username, refreshToken);
        result = 31 * result + Arrays.hashCode(password);
        result = 31 * result + Arrays.hashCode(kerberosTicket);
        return result;
    }
}
