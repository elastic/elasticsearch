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

import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a decision to move a started shard because it is no longer allowed to remain on its current node.
 */
public final class MoveDecision extends RelocationDecision {
    /** a constant representing no decision taken */
    public static final MoveDecision NOT_TAKEN = new MoveDecision(null, null, null, null, null);
    /** cached decisions so we don't have to recreate objects for common decisions when not in explain mode. */
    private static final MoveDecision CACHED_STAY_DECISION = new MoveDecision(Decision.YES, Decision.Type.NO, null, null, null);
    private static final MoveDecision CACHED_CANNOT_MOVE_DECISION = new MoveDecision(Decision.NO, Decision.Type.NO, null, null, null);

    @Nullable
    private final Decision canRemainDecision;
    @Nullable
    private final Map<String, NodeAllocationResult> nodeDecisions;

    private MoveDecision(Decision canRemainDecision, Decision.Type finalDecision, String finalExplanation,
                         String assignedNodeId, Map<String, NodeAllocationResult> nodeDecisions) {
        super(finalDecision, finalExplanation, assignedNodeId);
        this.canRemainDecision = canRemainDecision;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
    }

    /**
     * Creates a move decision for the shard being able to remain on its current node, so not moving.
     */
    public static MoveDecision stay(Decision canRemainDecision, boolean explain) {
        assert canRemainDecision.type() != Decision.Type.NO;
        if (explain) {
            final String explanation;
            if (explain) {
                explanation = "shard is allowed to remain on its current node, so no reason to move";
            } else {
                explanation = null;
            }
            return new MoveDecision(Objects.requireNonNull(canRemainDecision), Decision.Type.NO, explanation, null, null);
        } else {
            return CACHED_STAY_DECISION;
        }
    }

    /**
     * Creates a move decision for the shard not being able to remain on its current node.
     *
     * @param canRemainDecision the decision for whether the shard is allowed to remain on its current node
     * @param finalDecision the decision of whether to move the shard to another node
     * @param explain true if in explain mode
     * @param currentNodeId the current node id where the shard is assigned
     * @param assignedNodeId the node id for where the shard can move to
     * @param nodeDecisions the node-level decisions that comprised the final decision, non-null iff explain is true
     * @return the {@link MoveDecision} for moving the shard to another node
     */
    public static MoveDecision decision(Decision canRemainDecision, Decision.Type finalDecision, boolean explain, String currentNodeId,
                                        String assignedNodeId, Map<String, NodeAllocationResult> nodeDecisions) {
        assert canRemainDecision != null;
        assert canRemainDecision.type() != Decision.Type.YES : "create decision with MoveDecision#stay instead";
        String finalExplanation = null;
        if (explain) {
            assert currentNodeId != null;
            if (finalDecision == Decision.Type.YES) {
                assert assignedNodeId != null;
                finalExplanation = "shard cannot remain on node [" + currentNodeId + "], moving to node [" + assignedNodeId + "]";
            } else if (finalDecision == Decision.Type.THROTTLE) {
                finalExplanation = "shard cannot remain on node [" + currentNodeId + "], throttled on moving to another node";
            } else {
                finalExplanation = "shard cannot remain on node [" + currentNodeId + "], but cannot be assigned to any other node";
            }
        }
        if (finalExplanation == null && finalDecision == Decision.Type.NO) {
            // the final decision is NO (no node to move the shard to) and we are not in explain mode, return a cached version
            return CACHED_CANNOT_MOVE_DECISION;
        } else {
            assert ((assignedNodeId == null) == (finalDecision != Decision.Type.YES));
            return new MoveDecision(canRemainDecision, finalDecision, finalExplanation, assignedNodeId, nodeDecisions);
        }
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node and can be moved, returns {@code false} otherwise.
     */
    public boolean move() {
        return cannotRemain() && getFinalDecisionType() == Decision.Type.YES;
    }

    /**
     * Returns {@code true} if the shard cannot remain on its current node.
     */
    public boolean cannotRemain() {
        return isDecisionTaken() && canRemainDecision.type() == Decision.Type.NO;
    }

    /**
     * Gets the individual node-level decisions that went into making the final decision as represented by
     * {@link #getFinalDecisionType()}.  The map that is returned has the node id as the key and a {@link NodeAllocationResult}.
     */
    @Nullable
    public Map<String, NodeAllocationResult> getNodeDecisions() {
        return nodeDecisions;
    }
}
