/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * The response object for {@code TransportDelegatePkiAuthenticationAction} containing the issued access token.
 */
public final class DelegatePkiAuthenticationResponse extends ActionResponse implements ToXContentObject {

    private static final ParseField ACCESS_TOKEN_FIELD = new ParseField("access_token");
    private static final ParseField TYPE_FIELD = new ParseField("type");
    private static final ParseField EXPIRES_IN_FIELD = new ParseField("expires_in");

    public static final ConstructingObjectParser<DelegatePkiAuthenticationResponse, Void> PARSER = new ConstructingObjectParser<>(
            "delegate_pki_response", true, a -> {
                final String accessToken = (String) a[0];
                final String type = (String) a[1];
                if (false == "Bearer".equals(type)) {
                    throw new IllegalArgumentException("Unknown token type [" + type + "], only [Bearer] type permitted");
                }
                final Long expiresIn = (Long) a[2];
                return new DelegatePkiAuthenticationResponse(accessToken, TimeValue.timeValueSeconds(expiresIn));
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), ACCESS_TOKEN_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), TYPE_FIELD);
        PARSER.declareLong(ConstructingObjectParser.constructorArg(), EXPIRES_IN_FIELD);
    }

    private String accessToken;
    private TimeValue expiresIn;

    DelegatePkiAuthenticationResponse() { }

    public DelegatePkiAuthenticationResponse(String accessToken, TimeValue expiresIn) {
        this.accessToken = Objects.requireNonNull(accessToken);
        // always store expiration in seconds because this is how we "serialize" to JSON and we need to parse back
        this.expiresIn = TimeValue.timeValueSeconds(Objects.requireNonNull(expiresIn).getSeconds());
    }

    public DelegatePkiAuthenticationResponse(StreamInput input) throws IOException {
        super(input);
        accessToken = input.readString();
        expiresIn = input.readTimeValue();
    }

    public String getAccessToken() {
        return accessToken;
    }

    public TimeValue getExpiresIn() {
        return expiresIn;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(accessToken);
        out.writeTimeValue(expiresIn);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DelegatePkiAuthenticationResponse that = (DelegatePkiAuthenticationResponse) o;
        return Objects.equals(accessToken, that.accessToken) &&
            Objects.equals(expiresIn, that.expiresIn);
    }

    @Override
    public int hashCode() {
        return Objects.hash(accessToken, expiresIn);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field(ACCESS_TOKEN_FIELD.getPreferredName(), accessToken)
            .field(TYPE_FIELD.getPreferredName(), "Bearer")
            .field(EXPIRES_IN_FIELD.getPreferredName(), expiresIn.getSeconds());
        return builder.endObject();
    }
}
