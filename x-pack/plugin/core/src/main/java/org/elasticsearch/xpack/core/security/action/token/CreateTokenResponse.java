/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.util.Objects;

/**
 * Response containing the token string that was generated from a token creation request. This
 * object also contains the scope and expiration date. If the scope was not provided or if the
 * provided scope matches the scope of the token, then the scope value is <code>null</code>
 */
public final class CreateTokenResponse extends ActionResponse implements ToXContentObject {

    private String tokenString;
    private TimeValue expiresIn;
    private String scope;
    private String refreshToken;
    private String kerberosAuthenticationResponseToken;
    private Authentication authentication;

    CreateTokenResponse() {}

    public CreateTokenResponse(StreamInput in) throws IOException {
        super(in);
        tokenString = in.readString();
        expiresIn = in.readTimeValue();
        scope = in.readOptionalString();
        refreshToken = in.readOptionalString();
        kerberosAuthenticationResponseToken = in.readOptionalString();
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_11_0)) {
            authentication = new Authentication(in);
        }
    }

    public CreateTokenResponse(
        String tokenString,
        TimeValue expiresIn,
        String scope,
        String refreshToken,
        String kerberosAuthenticationResponseToken,
        Authentication authentication
    ) {
        this.tokenString = Objects.requireNonNull(tokenString);
        this.expiresIn = Objects.requireNonNull(expiresIn);
        this.scope = scope;
        this.refreshToken = refreshToken;
        this.kerberosAuthenticationResponseToken = kerberosAuthenticationResponseToken;
        this.authentication = authentication;
    }

    public String getTokenString() {
        return tokenString;
    }

    public String getScope() {
        return scope;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public String getKerberosAuthenticationResponseToken() {
        return kerberosAuthenticationResponseToken;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tokenString);
        out.writeTimeValue(expiresIn);
        out.writeOptionalString(scope);
        out.writeOptionalString(refreshToken);
        out.writeOptionalString(kerberosAuthenticationResponseToken);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_11_0)) {
            authentication.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("access_token", tokenString).field("type", "Bearer").field("expires_in", expiresIn.seconds());
        if (refreshToken != null) {
            builder.field("refresh_token", refreshToken);
        }
        // only show the scope if it is not null
        if (scope != null) {
            builder.field("scope", scope);
        }
        if (kerberosAuthenticationResponseToken != null) {
            builder.field("kerberos_authentication_response_token", kerberosAuthenticationResponseToken);
        }
        if (authentication != null) {
            builder.field("authentication", authentication);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateTokenResponse that = (CreateTokenResponse) o;
        return Objects.equals(tokenString, that.tokenString)
            && Objects.equals(expiresIn, that.expiresIn)
            && Objects.equals(scope, that.scope)
            && Objects.equals(refreshToken, that.refreshToken)
            && Objects.equals(kerberosAuthenticationResponseToken, that.kerberosAuthenticationResponseToken)
            && Objects.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenString, expiresIn, scope, refreshToken, kerberosAuthenticationResponseToken, authentication);
    }
}
