/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.authc.Authentication;

import java.io.IOException;
import java.util.Objects;

public class AuthenticateResponse extends ActionResponse implements ToXContent {

    public static final TransportVersion VERSION_OPERATOR_FIELD = TransportVersion.V_8_500_028;

    private final Authentication authentication;
    private final boolean operator;

    public AuthenticateResponse(StreamInput in) throws IOException {
        super(in);
        authentication = new Authentication(in);
        if (in.getTransportVersion().onOrAfter(VERSION_OPERATOR_FIELD)) {
            operator = in.readBoolean();
        } else {
            operator = false;
        }
    }

    public AuthenticateResponse(Authentication authentication, boolean operator) {
        this.authentication = Objects.requireNonNull(authentication);
        this.operator = operator;
    }

    public Authentication authentication() {
        return authentication;
    }

    public boolean isOperator() {
        return operator;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        authentication.writeTo(out);
        if (out.getTransportVersion().onOrAfter(VERSION_OPERATOR_FIELD)) {
            out.writeBoolean(operator);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        authentication.toXContentFragment(builder);
        if (this.operator) {
            builder.field("operator", true);
        }
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        AuthenticateResponse that = (AuthenticateResponse) o;
        return this.operator == that.operator && this.authentication.equals(that.authentication);
    }

    @Override
    public int hashCode() {
        return Objects.hash(authentication, operator);
    }
}
