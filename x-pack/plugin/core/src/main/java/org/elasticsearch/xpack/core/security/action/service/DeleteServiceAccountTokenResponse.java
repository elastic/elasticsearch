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

public class DeleteServiceAccountTokenResponse extends ActionResponse implements ToXContentObject {

    private boolean found;

    public DeleteServiceAccountTokenResponse(boolean found) {
        this.found = found;
    }

    public DeleteServiceAccountTokenResponse(StreamInput in) throws IOException {
        super(in);
        this.found = in.readBoolean();
    }

    public boolean found() {
        return this.found;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteServiceAccountTokenResponse that = (DeleteServiceAccountTokenResponse) o;
        return found == that.found;
    }

    @Override
    public int hashCode() {
        return Objects.hash(found);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("found", found).endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(found);
    }
}
