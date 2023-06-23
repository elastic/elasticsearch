/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

class NodeDecisions implements ToXContentObject, Writeable {

    private final List<NodeDecision> canAllocateDecisions;
    @Nullable
    private final NodeDecision canRemainDecision;

    NodeDecisions(List<NodeDecision> canAllocateDecisions, @Nullable NodeDecision canRemainDecision) {
        this.canAllocateDecisions = canAllocateDecisions;
        this.canRemainDecision = canRemainDecision;
    }

    NodeDecisions(StreamInput in) throws IOException {
        canAllocateDecisions = in.readList(NodeDecision::new);
        canRemainDecision = in.readOptionalWriteable(NodeDecision::new);
    }

    List<NodeDecision> canAllocateDecisions() {
        return canAllocateDecisions;
    }

    @Nullable
    NodeDecision canRemainDecision() {
        return canRemainDecision;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(canAllocateDecisions);
        out.writeOptionalWriteable(canRemainDecision);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.xContentList("can_allocate_decisions", canAllocateDecisions);
        if (canRemainDecision != null) {
            builder.field("can_remain_decision", canRemainDecision);
        }
        builder.endObject();
        return null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDecisions that = (NodeDecisions) o;
        return Objects.equals(canAllocateDecisions, that.canAllocateDecisions) && Objects.equals(canRemainDecision, that.canRemainDecision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(canAllocateDecisions, canRemainDecision);
    }
}
