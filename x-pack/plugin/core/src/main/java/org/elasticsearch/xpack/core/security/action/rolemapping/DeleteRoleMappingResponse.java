/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.rolemapping;

import java.io.IOException;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * Response for a role-mapping being deleted from the
 * org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore
 */
public class DeleteRoleMappingResponse extends ActionResponse implements ToXContentObject {

    private boolean found = false;

    /**
     * Package private for {@link DeleteRoleMappingAction#newResponse()}
     */
    public DeleteRoleMappingResponse() {}

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
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        found = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(found);
    }

}
