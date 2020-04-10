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
 * Response when adding a user to the security index. Returns a
 * single boolean field for whether the user was created or updated.
 */
public class PutUserResponse extends ActionResponse implements ToXContentObject {

    private boolean created;

    public PutUserResponse(StreamInput in) throws IOException {
        super(in);
        this.created = in.readBoolean();
    }

    public PutUserResponse(boolean created) {
        this.created = created;
    }

    public boolean created() {
        return created;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(created);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
            .field("created", created)
            .endObject();
    }
}
