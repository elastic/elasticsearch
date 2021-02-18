/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class NodeAcknowledgedResponse extends AcknowledgedResponse {

    public static final String NODE_FIELD = "node";

    private final String node;

    public NodeAcknowledgedResponse(boolean acknowledged, String node) {
        super(acknowledged);
        this.node = Objects.requireNonNull(node);
    }

    public NodeAcknowledgedResponse(StreamInput in) throws IOException {
        super(in);
        node = in.readString();
    }

    public String getNode() {
        return node;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(node);
    }

    @Override
    protected void addCustomFields(XContentBuilder builder, Params params) throws IOException {
        builder.field(NODE_FIELD, node);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeAcknowledgedResponse that = (NodeAcknowledgedResponse) o;
        return isAcknowledged() == that.isAcknowledged()
            && Objects.equals(node, that.node);
    }

    @Override
    public int hashCode() {
        return Objects.hash(isAcknowledged(), node);
    }
}
