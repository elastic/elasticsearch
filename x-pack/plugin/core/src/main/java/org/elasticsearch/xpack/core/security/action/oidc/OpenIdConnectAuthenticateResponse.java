/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;

public class OpenIdConnectAuthenticateResponse extends ActionResponse {
    private String principal;
    private String accessTokenString;
    private String refreshTokenString;
    private TimeValue expiresIn;
    private Authentication authentication;

    public OpenIdConnectAuthenticateResponse(
        Authentication authentication,
        String accessTokenString,
        String refreshTokenString,
        TimeValue expiresIn
    ) {
        this.principal = authentication.getEffectiveSubject().getUser().principal();
        ;
        this.accessTokenString = accessTokenString;
        this.refreshTokenString = refreshTokenString;
        this.expiresIn = expiresIn;
        this.authentication = authentication;
    }

    public OpenIdConnectAuthenticateResponse(StreamInput in) throws IOException {
        super(in);
        principal = in.readString();
        accessTokenString = in.readString();
        refreshTokenString = in.readString();
        expiresIn = in.readTimeValue();
        if (in.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication = new Authentication(in);
        }
    }

    public String getPrincipal() {
        return principal;
    }

    public String getAccessTokenString() {
        return accessTokenString;
    }

    public String getRefreshTokenString() {
        return refreshTokenString;
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
        out.writeString(accessTokenString);
        out.writeString(refreshTokenString);
        out.writeTimeValue(expiresIn);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication.writeTo(out);
        }
    }
}
