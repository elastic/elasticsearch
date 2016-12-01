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
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * An abstract class for representing various types of allocation decisions.
 */
public abstract class AbstractAllocationDecision implements ToXContent, Writeable {

    @Nullable
    private final Type decision;
    @Nullable
    private final DiscoveryNode assignedNode;
    @Nullable
    private final Map<String, NodeAllocationResult> nodeDecisions;

    public AbstractAllocationDecision(@Nullable Type decision, @Nullable DiscoveryNode assignedNode,
                                      @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
        this.decision = decision;
        this.assignedNode = assignedNode;
        this.nodeDecisions = nodeDecisions != null ? sortNodeDecisions(nodeDecisions) : null;
    }

    public AbstractAllocationDecision(StreamInput in) throws IOException {
        decision = in.readOptionalWriteable(Type::readFrom);
        assignedNode = in.readOptionalWriteable(DiscoveryNode::new);
        nodeDecisions = in.readBoolean() ? Collections.unmodifiableMap(
            in.readMap(StreamInput::readString, NodeAllocationResult::new, LinkedHashMap::new)) : null;
    }

    /**
     * Returns <code>true</code> if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object are meaningless and return {@code null}.
     */
    public boolean isDecisionTaken() {
        return decision != null;
    }

    /**
     * Returns the decision made by the allocator on whether to assign the shard.
     * This value can only be {@code null} if {@link #isDecisionTaken()} returns {@code false}.
     */
    @Nullable
    public Type getDecisionType() {
        return decision;
    }

    /**
     * Returns the decision made by the allocator on whether to assign the shard.
     * Only call this method if {@link #isDecisionTaken()} returns {@code true}, otherwise it will
     * throw an {@code IllegalArgumentException}.
     */
    public Type getDecisionTypeSafe() {
        if (isDecisionTaken() == false) {
            throw new IllegalArgumentException("decision must have been taken in order to return the final decision");
        }
        return decision;
    }

    /**
     * Get the node that the allocator will assign the shard to, unless {@link #getDecisionType()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public DiscoveryNode getAssignedNode() {
        return assignedNode;
    }

    /**
     * Gets the individual node-level decisions that went into making the final decision as represented by
     * {@link #getDecisionType()}.  The map that is returned has the node id as the key and a {@link Decision}
     * as the decision for the given node.
     */
    @Nullable
    public Map<String, NodeAllocationResult> getNodeDecisions() {
        return nodeDecisions;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isDecisionTaken() == false) {
            // no decision taken, so nothing meaningful to output
            return builder;
        }
        builder.field("decision", decision.toString());
        builder.field("explanation", getExplanation());
        if (assignedNode != null) {
            builder.startObject("assigned_node");
            discoveryNodeToXContent(builder, params, assignedNode);
            builder.endObject();
        }
        innerToXContent(builder, params);
        nodeDecisionsToXContent(builder, params, nodeDecisions);
        return builder;
    }

    /**
     * Gets the explanation for the decision
     */
    public abstract String getExplanation();

    /**
     * Implementation of each sub-classes x-content that is added to the x-content generated by the abstract parent class.
     */
    protected abstract void innerToXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(decision);
        out.writeOptionalWriteable(assignedNode);
        if (nodeDecisions != null) {
            out.writeBoolean(true);
            out.writeVInt(nodeDecisions.size());
            for (Map.Entry<String, NodeAllocationResult> entry : nodeDecisions.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        } else {
            out.writeBoolean(false);
        }
    }

    /**
     * Generates X-Content for a {@link DiscoveryNode} that leaves off some of the non-critical fields,
     * and assumes the outer object is created outside of this method call.
     */
    public static XContentBuilder discoveryNodeToXContent(XContentBuilder builder, ToXContent.Params params, DiscoveryNode node)
        throws IOException {

        builder.field("id", node.getId());
        builder.field("name", node.getName());
        builder.field("transport_address", node.getAddress().toString());
        builder.startObject("attributes");
        for (Map.Entry<String, String> entry : node.getAttributes().entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    /**
     * Sorts a map of node level decisions by the decision type, then by weight ranking, and finally by node id.
     */
    public Map<String, NodeAllocationResult> sortNodeDecisions(Map<String, NodeAllocationResult> nodeDecisions) {
        return Collections.unmodifiableMap(
            nodeDecisions.values().stream()
                .sorted()
                .collect(Collectors.toMap(r -> r.getNode().getId(),
                    Function.identity(),
                    (r1, r2) -> { throw new IllegalArgumentException(String.format(Locale.ROOT, "Duplicate key %s", r1)); },
                    LinkedHashMap::new))
        );
    }

    /**
     * Generates X-Content for the node-level decisions, creating the outer "node_decisions" object
     * in which they are serialized.
     */
    public XContentBuilder nodeDecisionsToXContent(XContentBuilder builder, ToXContent.Params params,
                                                   Map<String, NodeAllocationResult> nodeDecisions) throws IOException {
        if (nodeDecisions != null) {
            builder.startObject("node_decisions");
            {
                for (String nodeId : nodeDecisions.keySet()) {
                    NodeAllocationResult explanation = nodeDecisions.get(nodeId);
                    explanation.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder;
    }
}
