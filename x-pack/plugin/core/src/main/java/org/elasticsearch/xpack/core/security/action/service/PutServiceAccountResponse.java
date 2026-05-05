/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class PutServiceAccountResponse extends ActionResponse implements ToXContentObject {

    private final boolean created;

    public PutServiceAccountResponse(boolean created) {
        this.created = created;
    }

    public PutServiceAccountResponse(StreamInput in) throws IOException {
        this.created = in.readBoolean();
    }

    public boolean isCreated() {
        return created;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(created);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("created", created).endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PutServiceAccountResponse that = (PutServiceAccountResponse) o;
        return created == that.created;
    }

    @Override
    public int hashCode() {
        return Objects.hash(created);
    }
}
