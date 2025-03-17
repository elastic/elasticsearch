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
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Response when adding one or more application privileges to the security index.
 * Returns a collection of the privileges that were created (by implication, any other privileges were updated).
 */
public final class PutPrivilegesResponse extends ActionResponse implements ToXContentObject {

    private final Map<String, List<String>> created;

    public PutPrivilegesResponse(StreamInput in) throws IOException {
        this.created = in.readImmutableMap(StreamInput::readStringCollectionAsList);
    }

    public PutPrivilegesResponse(Map<String, List<String>> created) {
        this.created = Collections.unmodifiableMap(created);
    }

    /**
     * Get a list of privileges that were created (as opposed to updated)
     * @return A map from <em>Application Name</em> to a {@code List} of <em>privilege names</em>
     */
    public Map<String, List<String>> created() {
        return created;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject().field("created", created).endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(created, StreamOutput::writeStringCollection);
    }

}
