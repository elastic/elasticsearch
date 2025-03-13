/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.privilege;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

/**
 * Response when deleting application privileges.
 * Returns a collection of privileges that were successfully found and deleted.
 */
public final class DeletePrivilegesResponse extends ActionResponse implements ToXContentObject {

    private final Set<String> found;

    public DeletePrivilegesResponse(StreamInput in) throws IOException {
        super(in);
        this.found = in.readCollectionAsImmutableSet(StreamInput::readString);
    }

    public DeletePrivilegesResponse(Collection<String> found) {
        this.found = Set.copyOf(found);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("found", found).endObject();
        return builder;
    }

    public Set<String> found() {
        return this.found;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(found);
    }

}
