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
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Represents a decision to move a started shard, either because it is no longer allowed to remain on its current node
 * or because moving it to another node will form a better cluster balance.
 */
public final class MoveDecision extends AbstractAllocationDecision {
    /** a constant representing no decision taken */
    public static final MoveDecision NOT_TAKEN = new MoveDecision(null, null, AllocationDecision.NO_ATTEMPT, null, null, 0);
    /** cached decisions so we don't have to recreate objects for common decisions when not in explain mode. */
    private static final MoveDecision CACHED_STAY_DECISION = new MoveDecision(
        Decision.YES,
        null,
        AllocationDecision.NO_ATTEMPT,
        null,
        null,
        0
    );
    private static final MoveDecision CACHED_CANNOT_MOVE_DECISION = new MoveDecision(
        Decision.NO,
        null,
        AllocationDecision.NO,
        null,
        null,
        0
    );

    @Nullable
    AllocationDecision allocationDecision;
    @Nullable
    private final Decision canRemainDecision;
    @Nullable
    private final Decision clusterRebalanceDecision;
    private final int currentNodeRanking;

    private MoveDecision(
        Decision canRemainDecision,
        Decision clusterRebalanceDecision,
        AllocationDecision allocationDecision,
        DiscoveryNode assignedNode,
        List<NodeAllocationResult> nodeDecisions,
        int currentNodeRanking
    ) {
        super(assignedNode, nodeDecisions);
        this.allocationDecision = allocationDecision;
        this.canRemainDecision = canRemainDecision;
        this.clusterRebalanceDecision = clusterRebalanceDecision;
        this.currentNodeRanking = currentNodeRanking;
    }

    public MoveDecision(StreamInput in) throws IOException {
        super(in);
        allocationDecision = in.readOptionalWriteable(AllocationDecision::readFrom);
        canRemainDecision = in.readOptionalWriteable(Decision::readFrom);
        clusterRebalanceDecision = in.readOptionalWriteable(Decision::readFrom);
        currentNodeRanking = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(allocationDecision);
        out.writeOptionalWriteable(canRemainDecision);
        out.writeOptionalWriteable(clusterRebalanceDecision);
        out.writeVInt(currentNodeRanking);
    }

    /**
     * Creates a move decision for the shard being able to remain on its current node, so the shard won't
     * be forced to move to another node.
     */
    public static MoveDecision stay(Decision canRemainDecision) {
        if (canRemainDecision != null) {
            assert canRemainDecision.type() != Type.NO;
            return new MoveDecision(canRemainDecision, null, AllocationDecision.NO_ATTEMPT, null, null, 0);
        } else {
            return CACHED_STAY_DECISION;
        }
    }

    /**
     * Creates a move decision for the shard not being allowed to remain on its current node.
     *
     * @param canRemainDecision the decision for whether the shard is allowed to remain on its current node
     * @param allocationDecision the {@link AllocationDecision} for moving the shard to another node
     * @param assignedNode the node where the shard should move to
     * @param nodeDecisions the node-level decisions that comprised the final decision, non-null iff explain is true
     * @return the {@link MoveDecision} for moving the shard to another node
     */
    public static MoveDecision cannotRemain(
        Decision canRemainDecision,
        AllocationDecision allocationDecision,
        DiscoveryNode assignedNode,
        List<NodeAllocationResult> nodeDecisions
    ) {
        assert canRemainDecision != null;
        assert canRemainDecision.type() != Type.YES : "create decision with MoveDecision#stay instead";
        if (nodeDecisions == null && allocationDecision == AllocationDecision.NO) {
            // the final decision is NO (no node to move the shard to) and we are not in explain mode, return a cached version
            return CACHED_CANNOT_MOVE_DECISION;
        } else {
            assert ((assignedNode == null) == (allocationDecision != AllocationDecision.YES));
            return new MoveDecision(canRemainDecision, null, allocationDecision, assignedNode, nodeDecisions, 0);
        }
    }

    /**
     * Creates a move decision for when rebalancing the shard is not allowed.
     */
    public static MoveDecision cannotRebalance(
        Decision canRebalanceDecision,
        AllocationDecision allocationDecision,
        int currentNodeRanking,
        List<NodeAllocationResult> nodeDecisions
    ) {
        return new MoveDecision(null, canRebalanceDecision, allocationDecision, null, nodeDecisions, currentNodeRanking);
    }

    /**
     * Creates a decision for whether to move the shard to a different node to form a better cluster balance.
     */
    public static MoveDecision rebalance(
        Decision canRebalanceDecision,
        AllocationDecision allocationDecision,
        @Nullable DiscoveryNode assignedNode,
        int currentNodeRanking,
        List<NodeAllocationResult> nodeDecisions
    ) {
        return new MoveDecision(null, canRebalanceDecision, allocationDecision, assignedNode, nodeDecisions, currentNodeRanking);
    }

    @Override
    public boolean isDecisionTaken() {
        return canRemainDecision != null || clusterRebalanceDecision != null;
    }

    /**
     * Creates a new move decision from this decision, plus adding a remain decision.
     */
    public MoveDecision withRemainDecision(Decision canRemainDecision) {
        return new MoveDecision(
            canRemainDecision,
            clusterRebalanceDecision,
            allocationDecision,
            targetNode,
            nodeDecisions,
            currentNodeRanking
        );
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node and can be moved,
     * returns {@code false} otherwise.  If {@link #isDecisionTaken()} returns {@code false},
     * then invoking this method will throw an {@code IllegalStateException}.
     */
    public boolean forceMove() {
        checkDecisionState();
        return canRemain() == false && allocationDecision == AllocationDecision.YES;
    }

    /**
     * Returns {@code true} if the shard can remain on its current node, returns {@code false} otherwise.
     * If {@link #isDecisionTaken()} returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public boolean canRemain() {
        checkDecisionState();
        return canRemainDecision.type() == Type.YES;
    }

    /**
     * Returns the decision for the shard being allowed to remain on its current node.  If {@link #isDecisionTaken()}
     * returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public Decision getCanRemainDecision() {
        checkDecisionState();
        return canRemainDecision;
    }

    /**
     * Returns {@code true} if the shard is allowed to be rebalanced to another node in the cluster,
     * returns {@code false} otherwise.  If {@link #getClusterRebalanceDecision()} returns {@code null}, then
     * the result of this method is meaningless, as no rebalance decision was taken.  If {@link #isDecisionTaken()}
     * returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    public boolean canRebalanceCluster() {
        checkDecisionState();
        return clusterRebalanceDecision != null && clusterRebalanceDecision.type() == Type.YES;
    }

    /**
     * Returns the decision for being allowed to rebalance the shard.  Invoking this method will return
     * {@code null} if {@link #canRemain()} ()} returns {@code false}, which means the node is not allowed to
     * remain on its current node, so the cluster is forced to attempt to move the shard to a different node,
     * as opposed to attempting to rebalance the shard if a better cluster balance is possible by moving it.
     * If {@link #isDecisionTaken()} returns {@code false}, then invoking this method will throw an
     * {@code IllegalStateException}.
     */
    @Nullable
    public Decision getClusterRebalanceDecision() {
        checkDecisionState();
        return clusterRebalanceDecision;
    }

    /**
     * Returns the {@link AllocationDecision} for moving this shard to another node.  If {@link #isDecisionTaken()} returns
     * {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public AllocationDecision getAllocationDecision() {
        return allocationDecision;
    }

    /**
     * Gets the current ranking of the node to which the shard is currently assigned, relative to the
     * other nodes in the cluster as reported in {@link NodeAllocationResult#getWeightRanking()}.  The
     * ranking will only return a meaningful positive integer if {@link #getClusterRebalanceDecision()} returns
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
        if (clusterRebalanceDecision != null) {
            // it was a decision to rebalance the shard, because the shard was allowed to remain on its current node
            if (allocationDecision == AllocationDecision.AWAITING_INFO) {
                return Explanations.Rebalance.AWAITING_INFO;
            }
            return switch (clusterRebalanceDecision.type()) {
                case NO -> atLeastOneNodeWithYesDecision()
                    ? Explanations.Rebalance.CANNOT_REBALANCE_CAN_ALLOCATE
                    : Explanations.Rebalance.CANNOT_REBALANCE_CANNOT_ALLOCATE;
                case THROTTLE -> Explanations.Rebalance.CLUSTER_THROTTLE;
                case YES -> {
                    if (getTargetNode() != null) {
                        if (allocationDecision == AllocationDecision.THROTTLED) {
                            yield Explanations.Rebalance.NODE_THROTTLE;
                        } else {
                            yield Explanations.Rebalance.YES;
                        }
                    } else {
                        yield Explanations.Rebalance.ALREADY_BALANCED;
                    }
                }
            };
        } else {
            // it was a decision to force move the shard
            assert canRemain() == false;
            return switch (allocationDecision) {
                case YES -> Explanations.Move.YES;
                case THROTTLED -> Explanations.Move.THROTTLED;
                case NO -> Explanations.Move.NO;
                case WORSE_BALANCE, AWAITING_INFO, ALLOCATION_DELAYED, NO_VALID_SHARD_COPY, NO_ATTEMPT -> {
                    assert false : allocationDecision;
                    yield allocationDecision.toString();
                }
            };
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        checkDecisionState();
        if (targetNode != null) {
            builder.startObject("target_node");
            discoveryNodeToXContent(targetNode, true, builder);
            builder.endObject();
        }
        builder.field("can_remain_on_current_node", canRemain() ? "yes" : "no");
        if (canRemain() == false && canRemainDecision.getDecisions().isEmpty() == false) {
            builder.startArray("can_remain_decisions");
            canRemainDecision.toXContent(builder, params);
            builder.endArray();
        }
        if (clusterRebalanceDecision != null) {
            AllocationDecision rebalanceDecision = AllocationDecision.fromDecisionType(clusterRebalanceDecision.type());
            builder.field("can_rebalance_cluster", rebalanceDecision);
            if (rebalanceDecision != AllocationDecision.YES && clusterRebalanceDecision.getDecisions().isEmpty() == false) {
                builder.startArray("can_rebalance_cluster_decisions");
                clusterRebalanceDecision.toXContent(builder, params);
                builder.endArray();
            }
        }
        if (clusterRebalanceDecision != null) {
            builder.field("can_rebalance_to_other_node", allocationDecision);
            builder.field("rebalance_explanation", getExplanation());
        } else {
            builder.field("can_move_to_other_node", forceMove() ? "yes" : "no");
            builder.field("move_explanation", getExplanation());
        }
        nodeDecisionsToXContent(nodeDecisions, builder, params);
        return builder;
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other) == false) {
            return false;
        }
        if (other instanceof MoveDecision == false) {
            return false;
        }
        MoveDecision that = (MoveDecision) other;
        return Objects.equals(allocationDecision, that.allocationDecision)
            && Objects.equals(canRemainDecision, that.canRemainDecision)
            && Objects.equals(clusterRebalanceDecision, that.clusterRebalanceDecision)
            && currentNodeRanking == that.currentNodeRanking;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(allocationDecision, canRemainDecision, clusterRebalanceDecision, currentNodeRanking);
    }

}
