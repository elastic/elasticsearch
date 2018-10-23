/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.time.Instant;

/**
 * Response for the successful creation of an api key
 */
public final class CreateApiKeyResponse extends ActionResponse {

    private String name;
    private String key;
    private Instant expiration;

    CreateApiKeyResponse() {}

    public CreateApiKeyResponse(String name, String key, Instant expiration) {
        this.name = name;
        this.key = key;
        this.expiration = expiration;
    }

    public CreateApiKeyResponse(StreamInput in) throws IOException {
        // TODO(jaymode) call super(in) once this is added in #34655
        this.name = in.readString();
        this.key = in.readString();
        this.expiration = in.readOptionalInstant();
    }

    public String getName() {
        return name;
    }

    public String getKey() {
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
        out.writeString(key);
        out.writeOptionalInstant(expiration);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        // TODO(jaymode) remove this once transport supports reading writeables
        //throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
        super.readFrom(in);
        this.name = in.readString();
        this.key = in.readString();
        this.expiration = in.readOptionalInstant();
    }
}
