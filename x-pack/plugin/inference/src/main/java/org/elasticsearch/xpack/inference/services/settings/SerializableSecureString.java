/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.settings;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class SerializableSecureString implements ToXContentFragment, Writeable {

    private final SecureString secureString;

    public SerializableSecureString(StreamInput in) throws IOException {
        secureString = in.readSecureString();
    }

    public SerializableSecureString(SecureString secureString) {
        this.secureString = Objects.requireNonNull(secureString);
    }

    public SerializableSecureString(String value) {
        secureString = new SecureString(value.toCharArray());
    }

    public SecureString getSecureString() {
        return secureString;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.value(secureString.toString());
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(secureString);
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        SerializableSecureString that = (SerializableSecureString) o;
        return Objects.equals(secureString, that.secureString);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(secureString);
    }
}
