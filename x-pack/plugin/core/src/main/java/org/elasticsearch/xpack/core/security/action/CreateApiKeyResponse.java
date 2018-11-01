/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.CharArrays;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.SecureString;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

/**
 * Response for the successful creation of an api key
 */
public final class CreateApiKeyResponse extends ActionResponse {

    private final String name;
    private final String id;
    private final SecureString key;
    private final Instant expiration;

    public CreateApiKeyResponse(String name, String id, SecureString key, Instant expiration) {
        this.name = name;
        this.id = id;
        this.key = key;
        this.expiration = expiration;
    }

    public CreateApiKeyResponse(StreamInput in) throws IOException {
        super(in);
        this.name = in.readString();
        this.id = in.readString();
        byte[] bytes = null;
        try {
            bytes = in.readByteArray();
            this.key = new SecureString(CharArrays.utf8BytesToChars(bytes));
        } finally {
            if (bytes != null) {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        this.expiration = in.readOptionalInstant();
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }

    public SecureString getKey() {
        return key;
    }

    @Nullable
    public Instant getExpiration() {
        return expiration;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(id);
        byte[] bytes = null;
        try {
            bytes = CharArrays.toUtf8Bytes(key.getChars());
            out.writeByteArray(bytes);
        } finally {
            if (bytes != null) {
                Arrays.fill(bytes, (byte) 0);
            }
        }
        out.writeOptionalInstant(expiration);
    }

    @Override
    public void readFrom(StreamInput in) {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }
}
