/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.cloud;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Releasable;

import java.io.IOException;

public record CloudCredential(SecureString value) implements Releasable, Writeable {

    public CloudCredential(StreamInput in) throws IOException {
        this(in.readSecureString());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeSecureString(value);
    }

    @Override
    public void close() {
        value.close();
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof CloudCredential(SecureString other) && value.equals(other);
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public String toString() {
        return "CloudCredential{value='::es_redacted::'}";
    }
}
