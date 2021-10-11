/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Response for a role-mapping being deleted from the
 * org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class DeleteRoleMappingResponse extends ActionResponse implements ToXContentObject {

    private boolean found = false;

    public DeleteRoleMappingResponse(StreamInput in) throws IOException {
        super(in);
        found = in.readBoolean();
    }

    public DeleteRoleMappingResponse(boolean found) {
        this.found = found;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("found", found).endObject();
        return builder;
    }

    /**
     * If <code>true</code>, indicates the {@link DeleteRoleMappingRequest#getName() named role-mapping} was found and deleted.
     * Otherwise, the role-mapping could not be found.
     */
    public boolean isFound() {
        return this.found;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(found);
    }

}
