/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.user;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response when deleting a native user. Returns a single boolean field for whether the user was
 * found (and deleted) or not found.
 */
public class DeleteUserResponse extends ActionResponse implements ToXContentObject {

    private boolean found;

    public DeleteUserResponse(StreamInput in) throws IOException {
        super(in);
        found = in.readBoolean();
    }

    public DeleteUserResponse(boolean found) {
        this.found = found;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("found", found).endObject();
        return builder;
    }

    public boolean found() {
        return this.found;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(found);
    }

}
