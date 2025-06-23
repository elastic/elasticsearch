/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;

/**
 * The response from converting a SAML assertion into a security token.
 * Actually nothing SAML specific in this...
 */
public final class SamlAuthenticateResponse extends ActionResponse {

    private final String principal;
    private final String tokenString;
    private final String refreshToken;
    private final String realm;
    private final TimeValue expiresIn;
    private final Authentication authentication;

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
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_0_0)) {
            out.writeString(realm);
        }
        out.writeString(tokenString);
        out.writeString(refreshToken);
        out.writeTimeValue(expiresIn);
        authentication.writeTo(out);
    }
}
