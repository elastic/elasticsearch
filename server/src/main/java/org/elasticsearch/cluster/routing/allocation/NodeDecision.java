/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

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

public class NodeDecision implements ToXContentObject, Writeable {

    private final DiscoveryNode node;
    private final Decision decision;

    public NodeDecision(DiscoveryNode node, Decision decision) {
        this.node = node;
        this.decision = decision;
    }

    public NodeDecision(StreamInput in) throws IOException {
        node = new DiscoveryNode(in);
        decision = Decision.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        node.writeTo(out);
        decision.writeTo(out);
    }

    public DiscoveryNode node() {
        return node;
    }

    public Decision decision() {
        return decision;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NodeDecision that = (NodeDecision) o;
        return Objects.equals(node, that.node) && Objects.equals(decision, that.decision);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, decision);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        discoveryNodeToXContent(node, false, builder);
        builder.field("node_decision", decision);
        builder.endObject();
        return builder;
    }

}
