/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;

/**
 * The response from converting a SAML assertion into a security token.
 * Actually nothing SAML specific in this...
 */
public final class SamlAuthenticateResponse extends ActionResponse {

    private String principal;
    private String tokenString;
    private String refreshToken;
    private String realm;
    private TimeValue expiresIn;
    private Authentication authentication;

    public SamlAuthenticateResponse(StreamInput in) throws IOException {
        super(in);
        principal = in.readString();
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            realm = in.readString();
        }
        tokenString = in.readString();
        refreshToken = in.readString();
        expiresIn = in.readTimeValue();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication = new Authentication(in);
        }
    }

    public SamlAuthenticateResponse(Authentication authentication, String tokenString, String refreshToken, TimeValue expiresIn) {
        this.principal = authentication.getEffectiveSubject().getUser().principal();
        this.realm = authentication.getEffectiveSubject().getRealm().getName();
        this.tokenString = tokenString;
        this.refreshToken = refreshToken;
        this.expiresIn = expiresIn;
        this.authentication = authentication;
    }

    public String getPrincipal() {
        return principal;
    }

    public String getRealm() {
        return realm;
    }

    public String getTokenString() {
        return tokenString;
    }

    public String getRefreshToken() {
        return refreshToken;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            out.writeString(realm);
        }
        out.writeString(tokenString);
        out.writeString(refreshToken);
        out.writeTimeValue(expiresIn);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication.writeTo(out);
        }
    }

}
