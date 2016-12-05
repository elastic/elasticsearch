/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * An abstract class for representing various types of allocation decisions.
 */
public abstract class AbstractAllocationDecision implements ToXContent, Writeable {

    @Nullable
    protected final Type decision;
    @Nullable
    protected final DiscoveryNode targetNode;
    @Nullable
    protected final List<NodeAllocationResult> nodeDecisions;

    protected AbstractAllocationDecision(@Nullable Type decision, @Nullable DiscoveryNode targetNode,
                                         @Nullable List<NodeAllocationResult> nodeDecisions) {
        this.decision = decision;
        this.targetNode = targetNode;
        this.nodeDecisions = nodeDecisions != null ? sortNodeDecisions(nodeDecisions) : null;
    }

    protected AbstractAllocationDecision(StreamInput in) throws IOException {
        decision = in.readOptionalWriteable(Type::readFrom);
        targetNode = in.readOptionalWriteable(DiscoveryNode::new);
        nodeDecisions = in.readBoolean() ? Collections.unmodifiableList(in.readList(NodeAllocationResult::new)) : null;
    }

    /**
     * Returns <code>true</code> if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object cannot be accessed and will
     * throw an {@code IllegalStateException}.
     */
    public boolean isDecisionTaken() {
        return decision != null;
    }

    /**
     * Returns the decision made by the allocator on whether to assign the shard.  If {@link #isDecisionTaken()}
     * returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public Type getDecisionType() {
        checkDecisionState();
        return decision;
    }

    /**
     * Get the node that the allocator will assign the shard to, unless {@link #getDecisionType()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.  If
     * {@link #isDecisionTaken()} returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public DiscoveryNode getTargetNode() {
        checkDecisionState();
        return targetNode;
    }

    /**
     * Gets the sorted list of individual node-level decisions that went into making the final decision as
     * represented by {@link #getDecisionType()}.  If {@link #isDecisionTaken()} returns {@code false}, then
     * invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public List<NodeAllocationResult> getNodeDecisions() {
        checkDecisionState();
        return nodeDecisions;
    }

    /**
     * Gets the explanation for the decision.  If {@link #isDecisionTaken()} returns {@code false}, then invoking
     * this method will throw an {@code IllegalStateException}.
     */
    public abstract String getExplanation();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(decision);
        out.writeOptionalWriteable(targetNode);
        if (nodeDecisions != null) {
            out.writeBoolean(true);
            out.writeList(nodeDecisions);
        } else {
            out.writeBoolean(false);
        }
    }

    protected void checkDecisionState() {
        if (isDecisionTaken() == false) {
            throw new IllegalStateException("decision was not taken, individual object fields cannot be accessed");
        }
    }

    /**
     * Generates X-Content for a {@link DiscoveryNode} that leaves off some of the non-critical fields.
     */
    public static XContentBuilder discoveryNodeToXContent(DiscoveryNode node, boolean outerObjectWritten, XContentBuilder builder)
        throws IOException {

        builder.field(outerObjectWritten ? "id" : "node_id", node.getId());
        builder.field(outerObjectWritten ? "name" : "node_name", node.getName());
        builder.field("transport_address", node.getAddress().toString());
        if (node.getAttributes().isEmpty() == false) {
            builder.startObject(outerObjectWritten ? "attributes" : "node_attributes");
            for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
                builder.field(entry.getKey(), entry.getValue());
            }
            builder.endObject();
        }
        return builder;
    }

    /**
     * Sorts a list of node level decisions by the decision type, then by weight ranking, and finally by node id.
     */
    public List<NodeAllocationResult> sortNodeDecisions(List<NodeAllocationResult> nodeDecisions) {
        return Collections.unmodifiableList(nodeDecisions.stream().sorted().collect(Collectors.toList()));
    }

    /**
     * Generates X-Content for the node-level decisions, creating the outer "node_decisions" object
     * in which they are serialized.
     */
    public XContentBuilder nodeDecisionsToXContent(List<NodeAllocationResult> nodeDecisions, XContentBuilder builder, Params params)
        throws IOException {

        if (nodeDecisions != null) {
            builder.startArray("node_decisions");
            {
                for (NodeAllocationResult explanation : nodeDecisions) {
                    explanation.toXContent(builder, params);
                }
            }
            builder.endArray();
        }
        return builder;
    }

}
