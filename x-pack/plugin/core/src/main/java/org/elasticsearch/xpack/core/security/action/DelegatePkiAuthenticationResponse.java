/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.util.Objects;

/**
 * The response object for {@code TransportDelegatePkiAuthenticationAction} containing the issued access token.
 */
public final class DelegatePkiAuthenticationResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField ACCESS_TOKEN_FIELD = new ParseField("access_token");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField EXPIRES_IN_FIELD = new ParseField("expires_in");
    private static final ParseField AUTHENTICATION = new ParseField("authentication");

    private String accessToken;
    private TimeValue expiresIn;
    private Authentication authentication;

    DelegatePkiAuthenticationResponse() { }

    public DelegatePkiAuthenticationResponse(String accessToken, TimeValue expiresIn, Authentication authentication) {
        this.accessToken = Objects.requireNonNull(accessToken);
        // always store expiration in seconds because this is how we "serialize" to JSON and we need to parse back
        this.expiresIn = TimeValue.timeValueSeconds(Objects.requireNonNull(expiresIn).getSeconds());
        this.authentication = authentication;
    }

    public DelegatePkiAuthenticationResponse(StreamInput input) throws IOException {
        super(input);
        accessToken = input.readString();
        expiresIn = input.readTimeValue();
        if (input.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication = new Authentication(input);
        }
    }

    public String getAccessToken() {
        return accessToken;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    public Authentication getAuthentication() {
        return authentication;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(accessToken);
        out.writeTimeValue(expiresIn);
        if (out.getVersion().onOrAfter(Version.V_7_11_0)) {
            authentication.writeTo(out);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiAuthenticationResponse that = (DelegatePkiAuthenticationResponse) o;
        return Objects.equals(accessToken, that.accessToken) &&
            Objects.equals(expiresIn, that.expiresIn) &&
            Objects.equals(authentication, that.authentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, expiresIn, authentication);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ACCESS_TOKEN_FIELD.getPreferredName(), accessToken);
        builder.field(TYPE_FIELD.getPreferredName(), "Bearer");
        builder.field(EXPIRES_IN_FIELD.getPreferredName(), expiresIn.getSeconds());
        if (authentication != null) {
            builder.field(AUTHENTICATION.getPreferredName(), authentication);
        }
        return builder.endObject();
    }
}
