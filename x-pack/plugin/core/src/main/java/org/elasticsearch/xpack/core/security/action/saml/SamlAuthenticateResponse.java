/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.saml;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

/**
 * The response from converting a SAML assertion into a security token.
 * Actually nothing SAML specific in this...
 */
public final class SamlAuthenticateResponse extends ActionResponse {

    private String principal;
    private String tokenString;
    private String refreshToken;
    private TimeValue expiresIn;

    public SamlAuthenticateResponse() {
    }

    public SamlAuthenticateResponse(String principal, String tokenString, String refreshToken, TimeValue expiresIn) {
        this.principal = principal;
        this.tokenString = tokenString;
        this.refreshToken = refreshToken;
        this.expiresIn = expiresIn;
    }

    public String getPrincipal() {
        return principal;
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(principal);
        out.writeString(tokenString);
        out.writeString(refreshToken);
        out.writeTimeValue(expiresIn);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        principal = in.readString();
        tokenString = in.readString();
        refreshToken = in.readString();
        expiresIn = in.readTimeValue();
    }
}
