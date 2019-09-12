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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Response when creating a new OAuth2 token in the Elasticsearch cluster. Contains an access token, the token's expiry, and an optional
 * refresh token.
 */
public final class CreateTokenResponse {

    private final String accessToken;
    private final String type;
    private final TimeValue expiresIn;
    private final String scope;
    private final String refreshToken;
    private final String kerberosAuthenticationResponseToken;

    public CreateTokenResponse(String accessToken, String type, TimeValue expiresIn, String scope, String refreshToken,
                               String kerberosAuthenticationResponseToken) {
        this.accessToken = accessToken;
        this.type = type;
        this.expiresIn = expiresIn;
        this.scope = scope;
        this.refreshToken = refreshToken;
        this.kerberosAuthenticationResponseToken = kerberosAuthenticationResponseToken;
    }

    public String getAccessToken() {
        return accessToken;
    }

    public String getType() {
        return type;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    public String getScope() {
        return scope;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getKerberosAuthenticationResponseToken() {
        return kerberosAuthenticationResponseToken;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CreateTokenResponse that = (CreateTokenResponse) o;
        return Objects.equals(accessToken, that.accessToken) &&
            Objects.equals(type, that.type) &&
            Objects.equals(expiresIn, that.expiresIn) &&
            Objects.equals(scope, that.scope) &&
            Objects.equals(refreshToken, that.refreshToken) &&
            Objects.equals(kerberosAuthenticationResponseToken, that.kerberosAuthenticationResponseToken);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, type, expiresIn, scope, refreshToken, kerberosAuthenticationResponseToken);
    }

    private static final ConstructingObjectParser<CreateTokenResponse, Void> PARSER = new ConstructingObjectParser<>(
            "create_token_response", true, args -> new CreateTokenResponse((String) args[0], (String) args[1],
                    TimeValue.timeValueSeconds((Long) args[2]), (String) args[3], (String) args[4], (String) args[5]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("access_token"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareLong(constructorArg(), new ParseField("expires_in"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("scope"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("refresh_token"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("kerberos_authentication_response_token"));
    }

    public static CreateTokenResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

