/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public final class KibanaEnrollmentResponse extends ActionResponse implements ToXContentObject {

    private final String tokenName;
    private final SecureString tokenValue;
    private final String httpCa;

    public KibanaEnrollmentResponse(StreamInput in) throws IOException {
        super(in);
        tokenName = in.readString();
        tokenValue = in.readSecureString();
        httpCa = in.readString();
    }

    public KibanaEnrollmentResponse(String tokenName, SecureString tokenValue, String httpCa) {
        this.tokenName = tokenName;
        this.tokenValue = tokenValue;
        this.httpCa = httpCa;
    }

    public String getTokenName() {
        return tokenName;
    }

    public SecureString getTokenValue() {
        return tokenValue;
    }

    public String getHttpCa() {
        return httpCa;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(tokenName);
        out.writeSecureString(tokenValue);
        out.writeString(httpCa);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KibanaEnrollmentResponse that = (KibanaEnrollmentResponse) o;
        return tokenName.equals(that.tokenName) && tokenValue.equals(that.tokenValue) && httpCa.equals(that.httpCa);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tokenName, tokenValue, httpCa);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .startObject("token")
            .field("name", tokenName)
            .field("value", tokenValue.toString())
            .endObject()
            .field("http_ca", httpCa)
            .endObject();
        return builder;
    }
}
