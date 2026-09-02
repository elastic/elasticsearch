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

/**
 * Reports whether the delete removed an account, in the same shape the delete service token API reports the same
 * thing.
 */
public class DeleteUserManagedServiceAccountResponse extends ActionResponse implements ToXContentObject {

    private final boolean found;

    public DeleteUserManagedServiceAccountResponse(boolean found) {
        this.found = found;
    }

    public DeleteUserManagedServiceAccountResponse(StreamInput in) throws IOException {
        this.found = in.readBoolean();
    }

    public boolean found() {
        return found;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(found);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field("found", found).endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DeleteUserManagedServiceAccountResponse that = (DeleteUserManagedServiceAccountResponse) o;
        return found == that.found;
    }

    @Override
    public int hashCode() {
        return Objects.hash(found);
    }
}
