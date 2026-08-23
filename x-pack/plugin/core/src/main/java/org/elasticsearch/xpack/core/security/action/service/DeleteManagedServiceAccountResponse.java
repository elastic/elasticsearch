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

public class DeleteManagedServiceAccountResponse extends ActionResponse implements ToXContentObject {

    private final boolean found;

    public DeleteManagedServiceAccountResponse(boolean found) {
        this.found = found;
    }

    public DeleteManagedServiceAccountResponse(StreamInput in) throws IOException {
        found = in.readBoolean();
    }

    public boolean isFound() {
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
}
