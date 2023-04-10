/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.shutdown;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class PrevalidateNodeRemovalResponse extends ActionResponse implements ToXContentObject {

    private final NodesRemovalPrevalidation prevalidation;

    public PrevalidateNodeRemovalResponse(final NodesRemovalPrevalidation prevalidation) {
        this.prevalidation = prevalidation;
    }

    public NodesRemovalPrevalidation getPrevalidation() {
        return prevalidation;
    }

    public PrevalidateNodeRemovalResponse(StreamInput in) throws IOException {
        super(in);
        prevalidation = NodesRemovalPrevalidation.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        prevalidation.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        prevalidation.toXContent(builder, params);
        return builder;
    }

    public static PrevalidateNodeRemovalResponse fromXContent(XContentParser parser) throws IOException {
        return new PrevalidateNodeRemovalResponse(NodesRemovalPrevalidation.PARSER.parse(parser, null));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof PrevalidateNodeRemovalResponse == false) return false;
        PrevalidateNodeRemovalResponse other = (PrevalidateNodeRemovalResponse) o;
        return Objects.equals(prevalidation, other.prevalidation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(prevalidation);
    }
}
