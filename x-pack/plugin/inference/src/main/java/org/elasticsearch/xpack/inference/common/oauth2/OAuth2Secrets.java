/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.oauth2;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A class representing the secrets required for the OAuth2 client credentials flow.
 */
public class OAuth2Secrets implements ToXContentObject, Writeable {

    public static final String CLIENT_SECRET_FIELD = "client_secret";

    private final SecureString clientSecret;

    public OAuth2Secrets(SecureString clientSecrets) {
        this.clientSecret = Objects.requireNonNull(clientSecrets);
    }

    public OAuth2Secrets(StreamInput in) throws IOException {
        this(in.readSecureString());
    }

    public SecureString clientSecret() {
        return clientSecret;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(clientSecret);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLIENT_SECRET_FIELD, clientSecret.toString());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        OAuth2Secrets that = (OAuth2Secrets) o;
        return Objects.equals(clientSecret, that.clientSecret);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientSecret);
    }
}
