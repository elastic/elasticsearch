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
import java.util.List;

/**
 * Represents a decision to move a started shard, either because it is no longer allowed to remain on its current node
 * or because moving it to another node will form a better cluster balance.
 */
public final class MoveDecision extends AbstractAllocationDecision {
    /** a constant representing no decision taken */
    public static final MoveDecision NOT_TAKEN = new MoveDecision(null, null, null, null, null, null, false, 0);
    /** cached decisions so we don't have to recreate objects for common decisions when not in explain mode. */
    private static final MoveDecision CACHED_STAY_DECISION =
        new MoveDecision(null, Decision.YES, null, Type.NO, null, null, false, 0);
    private static final MoveDecision CACHED_CANNOT_MOVE_DECISION =
        new MoveDecision(null, Decision.NO, null, Type.NO, null, null, false, 0);

    @Nullable
    private final DiscoveryNode currentNode;
    @Nullable
    private final Decision canRemainDecision;
    @Nullable
    private final Decision canRebalanceDecision;
    private final boolean fetchPending;
    private final int currentNodeRanking;

    private MoveDecision(DiscoveryNode currentNode, Decision canRemainDecision, Decision canRebalanceDecision, Type finalDecision,
                         DiscoveryNode assignedNode, List<NodeAllocationResult> nodeDecisions, boolean fetchPending,
                         int currentNodeRanking) {
        super(finalDecision, assignedNode, nodeDecisions);
        this.currentNode = currentNode;
        this.canRemainDecision = canRemainDecision;
        this.canRebalanceDecision = canRebalanceDecision;
        this.fetchPending = fetchPending;
        this.currentNodeRanking = currentNodeRanking;
    }

    public MoveDecision(StreamInput in) throws IOException {
        super(in);
        currentNode = in.readOptionalWriteable(DiscoveryNode::new);
        canRemainDecision = in.readOptionalWriteable(Decision::readFrom);
        canRebalanceDecision = in.readOptionalWriteable(Decision::readFrom);
        fetchPending = in.readBoolean();
        currentNodeRanking = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(currentNode);
        out.writeOptionalWriteable(canRemainDecision);
        out.writeOptionalWriteable(canRebalanceDecision);
        out.writeBoolean(fetchPending);
        out.writeVInt(currentNodeRanking);
    }

    /**
     * Creates a move decision for the shard being able to remain on its current node, so the shard won't
     * be forced to move to another node.
     */
    public static MoveDecision stay(DiscoveryNode currentNode, Decision canRemainDecision) {
        if (canRemainDecision != null) {
            assert canRemainDecision.type() != Type.NO;
            return new MoveDecision(currentNode, canRemainDecision, null, Type.NO, null, null, false, 0);
        } else {
            return CACHED_STAY_DECISION;
        }
    }

    /**
     * Creates a move decision for the shard not being allowed to remain on its current node.
     *
     * @param currentNode the current node on which the shard resides
     * @param canRemainDecision the decision for whether the shard is allowed to remain on its current node
     * @param finalDecision the decision of whether to move the shard to another node
     * @param assignedNode the node for where the shard can move to
     * @param nodeDecisions the node-level decisions that comprised the final decision, non-null iff explain is true
     * @return the {@link MoveDecision} for moving the shard to another node
     */
    public static MoveDecision cannotRemain(DiscoveryNode currentNode, Decision canRemainDecision, Type finalDecision,
                                            DiscoveryNode assignedNode, List<NodeAllocationResult> nodeDecisions) {
        assert canRemainDecision != null;
        assert canRemainDecision.type() != Type.YES : "create decision with MoveDecision#stay instead";
        if (nodeDecisions == null && finalDecision == Type.NO) {
            // the final decision is NO (no node to move the shard to) and we are not in explain mode, return a cached version
            return CACHED_CANNOT_MOVE_DECISION;
        } else {
            assert ((assignedNode == null) == (finalDecision != Type.YES));
            return new MoveDecision(currentNode, canRemainDecision, null, finalDecision, assignedNode, nodeDecisions, false, 0);
        }
    }

    /**
     * Creates a move decision for when rebalancing the shard is not allowed.
     */
    public static MoveDecision cannotRebalance(DiscoveryNode currentNode, Decision canRebalanceDecision, int currentNodeRanking,
                                               List<NodeAllocationResult> nodeDecisions, boolean fetchPending) {
        return new MoveDecision(currentNode, null, canRebalanceDecision, Type.NO, null, nodeDecisions, fetchPending, currentNodeRanking);
    }

    /**
     * Creates a decision for whether to move the shard to a different node to form a better cluster balance.
     */
    public static MoveDecision rebalance(DiscoveryNode currentNode, Decision canRebalanceDecision, Type finalDecision,
                                         @Nullable DiscoveryNode assignedNode, int currentNodeRanking,
                                         List<NodeAllocationResult> nodeDecisions) {
        return new MoveDecision(currentNode, null, canRebalanceDecision, finalDecision, assignedNode, nodeDecisions,
                                   false, currentNodeRanking);
    }

    /**
     * Gets the current node to which the shard is assigned.  If {@link #isDecisionTaken()} returns
     * {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public DiscoveryNode getCurrentNode() {
        checkDecisionState();
        return currentNode;
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node and can be moved,
     * returns {@code false} otherwise.  If {@link #isDecisionTaken()} returns {@code false},
     * then invoking this method will throw an {@code IllegalStateException}.
     */
    public boolean forceMove() {
        checkDecisionState();
        return cannotRemain() && getDecisionType() == Type.YES;
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node.  If {@link #isDecisionTaken()} returns
     * {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public boolean cannotRemain() {
        checkDecisionState();
        return isDecisionTaken() && canRemainDecision.type() == Type.NO;
    }

    /**
     * Returns the decision for being allowed to rebalance the shard.  Invoking this method will return a
     * {@code null} if {@link #cannotRemain()} returns {@code true}, which means the node is not allowed to
     * remain on its current node, so the cluster is forced to attempt to move the shard to a different node,
     * as opposed to attempting to rebalance the shard if a better cluster balance is possible by moving it.
     * If {@link #isDecisionTaken()} returns {@code false}, then invoking this method will throw an
     * {@code IllegalStateException}.
     */
    @Nullable
    public Decision getCanRebalanceDecision() {
        checkDecisionState();
        return canRebalanceDecision;
    }

    /**
     * Gets the current ranking of the node to which the shard is currently assigned, relative to the
     * other nodes in the cluster as reported in {@link NodeAllocationResult#getWeightRanking()}.  The
     * ranking will only return a meaningful positive integer if {@link #getCanRebalanceDecision()} returns
     * a non-null value; otherwise, 0 will be returned.  If {@link #isDecisionTaken()} returns
     * {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public int getCurrentNodeRanking() {
        checkDecisionState();
        return currentNodeRanking;
    }

    @Override
    public String getExplanation() {
        checkDecisionState();
        String explanation;
        if (canRebalanceDecision != null) {
            // it was a decision to rebalance the shard, because the shard was allowed to remain on its current node
            if (fetchPending) {
                explanation = "cannot rebalance as information about existing copies of this shard in the cluster is still being gathered";
            } else if (canRebalanceDecision.type() == Type.NO) {
                explanation = "rebalancing is not allowed";
            } else if (canRebalanceDecision.type() == Type.THROTTLE) {
                explanation = "rebalancing is throttled";
            } else {
                if (getTargetNode() != null) {
                    if (getDecisionType() == Type.THROTTLE) {
                        explanation = "shard rebalancing throttled";
                    } else {
                        explanation = "can rebalance shard";
                    }
                } else {
                    explanation = "cannot rebalance as no target node node exists that can both allocate this shard " +
                                      "and improve the cluster balance";
                }
            }
        } else {
            // it was a decision to force move the shard
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
        }
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        checkDecisionState();
        builder.startObject("current_node");
        {
            discoveryNodeToXContent(currentNode, true, builder);
            if (currentNodeRanking > 0) {
                builder.field("weight_ranking", currentNodeRanking);
            }
        }
        builder.endObject();
        builder.field(canRebalanceDecision != null ? "rebalance_decision" : "move_decision", decision.toString());
        builder.field("explanation", getExplanation());
        if (targetNode != null) {
            builder.startObject("target_node");
            discoveryNodeToXContent(targetNode, true, builder);
            builder.endObject();
        }
        builder.field("can_remain_decision", canRemainDecision.type().toString());
        if (canRemainDecision.getDecisions().isEmpty() == false) {
            builder.startArray("can_remain_details");
            canRemainDecision.toXContent(builder, params);
            builder.endArray();
        }
        if (canRebalanceDecision != null) {
            builder.field("can_rebalance_decision", canRebalanceDecision.type().toString());
            if (canRebalanceDecision.getDecisions().isEmpty() == false) {
                builder.startArray("can_rebalance_details");
                canRebalanceDecision.toXContent(builder, params);
                builder.endArray();
            }
        }
        nodeDecisionsToXContent(nodeDecisions, builder, params);
        return builder;
    }

}
