/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling.storage;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.cluster.routing.allocation.AbstractAllocationDecision.discoveryNodeToXContent;

class NodeDecision implements ToXContentObject, Writeable {

    private final DiscoveryNode node;
    private final Decision decision;

    NodeDecision(DiscoveryNode node, Decision decision) {
        this.node = node;
        this.decision = decision;
    }

    NodeDecision(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        decision = Decision.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        decision.writeTo(out);
    }

    DiscoveryNode node() {
        return node;
    }

    Decision decision() {
        return decision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        return o instanceof NodeDecision that && Objects.equals(node, that.node) && Objects.equals(decision, that.decision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, decision);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        discoveryNodeToXContent(node, false, builder);
        {
            builder.startArray("deciders");
            decision.toXContent(builder, params);
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }

}
