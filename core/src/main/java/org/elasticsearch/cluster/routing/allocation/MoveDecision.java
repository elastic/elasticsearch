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
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

/**
 * Represents a decision to move a started shard because it is no longer allowed to remain on its current node.
 */
public final class MoveDecision extends AbstractAllocationDecision {
    /** a constant representing no decision taken */
    public static final MoveDecision NOT_TAKEN = new MoveDecision(null, null, null, null, null);
    /** cached decisions so we don't have to recreate objects for common decisions when not in explain mode. */
    private static final MoveDecision CACHED_STAY_DECISION = new MoveDecision(null, Decision.YES, Type.NO, null, null);
    private static final MoveDecision CACHED_CANNOT_MOVE_DECISION = new MoveDecision(null, Decision.NO, Type.NO, null, null);

    @Nullable
    private final DiscoveryNode currentNode;
    @Nullable
    private final Decision canRemainDecision;

    private MoveDecision(DiscoveryNode currentNode, Decision canRemainDecision, Type finalDecision,
                         DiscoveryNode assignedNode, Map<String, NodeAllocationResult> nodeDecisions) {
        super(finalDecision, assignedNode, nodeDecisions);
        this.currentNode = currentNode;
        this.canRemainDecision = canRemainDecision;
    }

    public MoveDecision(StreamInput in) throws IOException {
        super(in);
        currentNode = in.readOptionalWriteable(DiscoveryNode::new);
        canRemainDecision = in.readOptionalWriteable(Decision::readFrom);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(currentNode);
        out.writeOptionalWriteable(canRemainDecision);
    }

    /**
     * Creates a move decision for the shard being able to remain on its current node, so not moving.
     */
    public static MoveDecision stay(DiscoveryNode currentNode, Decision canRemainDecision) {
        if (canRemainDecision != null) {
            assert canRemainDecision.type() != Type.NO;
            return new MoveDecision(currentNode, canRemainDecision, Type.NO, null, null);
        } else {
            return CACHED_STAY_DECISION;
        }
    }

    /**
     * Creates a move decision for the shard not being able to remain on its current node.
     *
     * @param currentNode the current node on which the shard resides
     * @param canRemainDecision the decision for whether the shard is allowed to remain on its current node
     * @param finalDecision the decision of whether to move the shard to another node
     * @param assignedNode the node for where the shard can move to
     * @param nodeDecisions the node-level decisions that comprised the final decision, non-null iff explain is true
     * @return the {@link MoveDecision} for moving the shard to another node
     */
    public static MoveDecision decision(DiscoveryNode currentNode, Decision canRemainDecision, Type finalDecision,
                                        DiscoveryNode assignedNode, Map<String, NodeAllocationResult> nodeDecisions) {
        assert canRemainDecision != null;
        assert canRemainDecision.type() != Type.YES : "create decision with MoveDecision#stay instead";
        if (nodeDecisions == null && finalDecision == Type.NO) {
            // the final decision is NO (no node to move the shard to) and we are not in explain mode, return a cached version
            return CACHED_CANNOT_MOVE_DECISION;
        } else {
            assert ((assignedNode == null) == (finalDecision != Type.YES));
            return new MoveDecision(currentNode, canRemainDecision, finalDecision, assignedNode, nodeDecisions);
        }
    }

    /**
     * Gets the current node to which the shard is assigned.
     */
    @Nullable
    public DiscoveryNode getCurrentNode() {
        return currentNode;
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node and can be moved, returns {@code false} otherwise.
     */
    public boolean move() {
        return cannotRemain() && getDecisionType() == Type.YES;
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node.
     */
    public boolean cannotRemain() {
        return isDecisionTaken() && canRemainDecision.type() == Type.NO;
    }

    /**
     * Gets the final explanation for the decision to move a shard.
     */
    @Override
    public String getExplanation() {
        String explanation;
        if (cannotRemain() == false) {
            explanation = "can remain on its current node";
        } else if (getDecisionType() == Type.YES) {
            explanation = "shard cannot remain on this node and is force-moved to another node";
        } else if (getDecisionType() == Type.THROTTLE) {
            explanation = "shard cannot remain on this node but is throttled on moving to another node";
        } else {
            assert getDecisionType() == Type.NO;
            explanation = "cannot move shard to another node, even though it is not allowed to remain on its current node";
        }
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (isDecisionTaken() == false) {
            // no decision taken, so nothing meaningful to output
            return builder;
        }
        assert currentNode != null : "current node should only be null if the decision was not taken";
        builder.startObject("current_node");
        {
            discoveryNodeToXContent(currentNode, builder, params);
        }
        builder.endObject();
        builder.field("decision", decision.toString());
        builder.field("explanation", getExplanation());
        if (targetNode != null) {
            builder.startObject("target_node");
            discoveryNodeToXContent(targetNode, builder, params);
            builder.endObject();
        }
        builder.startObject("can_remain_decision");
        {
            builder.field("decision", canRemainDecision.type().toString());
            canRemainDecision.toXContent(builder, params);
        }
        builder.endObject();
        nodeDecisionsToXContent(nodeDecisions, builder, params);
        return builder;
    }

}
