/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.TimeValue;
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
    private final AuthenticateResponse authentication;

    public CreateTokenResponse(String accessToken, String type, TimeValue expiresIn, String scope, String refreshToken,
                               String kerberosAuthenticationResponseToken, AuthenticateResponse authentication) {
        this.accessToken = accessToken;
        this.type = type;
        this.expiresIn = expiresIn;
        this.scope = scope;
        this.refreshToken = refreshToken;
        this.kerberosAuthenticationResponseToken = kerberosAuthenticationResponseToken;
        this.authentication = authentication;
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

    public AuthenticateResponse getAuthentication() { return authentication; }

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
            Objects.equals(kerberosAuthenticationResponseToken, that.kerberosAuthenticationResponseToken)&&
            Objects.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, type, expiresIn, scope, refreshToken, kerberosAuthenticationResponseToken, authentication);
    }

    private static final ConstructingObjectParser<CreateTokenResponse, Void> PARSER = new ConstructingObjectParser<>(
            "create_token_response", true, args -> new CreateTokenResponse((String) args[0], (String) args[1],
                    TimeValue.timeValueSeconds((Long) args[2]), (String) args[3], (String) args[4], (String) args[5],
                    (AuthenticateResponse) args[6]));

    static {
        PARSER.declareString(constructorArg(), new ParseField("access_token"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareLong(constructorArg(), new ParseField("expires_in"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("scope"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("refresh_token"));
        PARSER.declareStringOrNull(optionalConstructorArg(), new ParseField("kerberos_authentication_response_token"));
        PARSER.declareObject(constructorArg(), (p, c) -> AuthenticateResponse.fromXContent(p), new ParseField("authentication"));
    }

    public static CreateTokenResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}

