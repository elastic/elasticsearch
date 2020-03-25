/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.oidc;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;

public class OpenIdConnectAuthenticateResponse extends ActionResponse {
    private String principal;
    private String accessTokenString;
    private String refreshTokenString;
    private TimeValue expiresIn;

    public OpenIdConnectAuthenticateResponse(String principal, String accessTokenString, String refreshTokenString, TimeValue expiresIn) {
        this.principal = principal;
        this.accessTokenString = accessTokenString;
        this.refreshTokenString = refreshTokenString;
        this.expiresIn = expiresIn;
    }

    public OpenIdConnectAuthenticateResponse(StreamInput in) throws IOException {
        super(in);
        principal = in.readString();
        accessTokenString = in.readString();
        refreshTokenString = in.readString();
        expiresIn = in.readTimeValue();
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(principal);
        out.writeString(accessTokenString);
        out.writeString(refreshTokenString);
        out.writeTimeValue(expiresIn);
    }
}
