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

import org.elasticsearch.cluster.routing.UnassignedInfo.AllocationStatus;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.Decision.Type;
import org.elasticsearch.common.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the allocation decision by an allocator for an unassigned shard.
 */
public class UnassignedShardDecision {
    /** a constant representing a shard decision where no decision was taken */
    public static final UnassignedShardDecision DECISION_NOT_TAKEN =
        new UnassignedShardDecision(null, null, null, null, null, null);

    @Nullable
    private final Decision finalDecision;
    @Nullable
    private final AllocationStatus allocationStatus;
    @Nullable
    private final String finalExplanation;
    @Nullable
    private final String assignedNodeId;
    @Nullable
    private final String allocationId;
    @Nullable
    private final Map<String, Decision> nodeDecisions;

    private UnassignedShardDecision(Decision finalDecision,
                                    AllocationStatus allocationStatus,
                                    String finalExplanation,
                                    String assignedNodeId,
                                    String allocationId,
                                    Map<String, Decision> nodeDecisions) {
        assert finalExplanation != null || finalDecision == null :
            "if a decision was taken, there must be an explanation for it";
        assert assignedNodeId != null || finalDecision == null || finalDecision.type() != Type.YES :
            "a yes decision must have a node to assign the shard to";
        assert allocationStatus != null || finalDecision == null || finalDecision.type() == Type.YES :
            "only a yes decision should not have an allocation status";
        assert allocationId == null || assignedNodeId != null :
            "allocation id can only be null if the assigned node is null";
        this.finalDecision = finalDecision;
        this.allocationStatus = allocationStatus;
        this.finalExplanation = finalExplanation;
        this.assignedNodeId = assignedNodeId;
        this.allocationId = allocationId;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
    }

    /**
     * Creates a NO decision with the given {@link AllocationStatus} and explanation for the NO decision.
     */
    public static UnassignedShardDecision noDecision(AllocationStatus allocationStatus, String explanation) {
        return noDecision(allocationStatus, explanation, null);
    }

    /**
     * Creates a NO decision with the given {@link AllocationStatus} and explanation for the NO decision,
     * as well as the individual node-level decisions that comprised the final NO decision.
     */
    public static UnassignedShardDecision noDecision(AllocationStatus allocationStatus,
                                                     String explanation,
                                                     @Nullable Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(explanation, "explanation must not be null");
        Objects.requireNonNull(allocationStatus, "allocationStatus must not be null");
        return new UnassignedShardDecision(Decision.NO, allocationStatus, explanation, null, null, nodeDecisions);
    }

    /**
     * Creates a THROTTLE decision with the given explanation and individual node-level decisions that
     * comprised the final THROTTLE decision.
     */
    public static UnassignedShardDecision throttleDecision(String explanation,
                                                           Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(explanation, "explanation must not be null");
        return new UnassignedShardDecision(Decision.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, explanation, null, null,
                                           nodeDecisions);
    }

    /**
     * Creates a YES decision with the given explanation and individual node-level decisions that
     * comprised the final YES decision, along with the node id to which the shard is assigned and
     * the allocation id for the shard, if available.
     */
    public static UnassignedShardDecision yesDecision(String explanation,
                                                      String assignedNodeId,
                                                      @Nullable String allocationId,
                                                      Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(explanation, "explanation must not be null");
        Objects.requireNonNull(assignedNodeId, "assignedNodeId must not be null");
        return new UnassignedShardDecision(Decision.YES, null, explanation, assignedNodeId, allocationId, nodeDecisions);
    }

    /**
     * Returns <code>true</code> if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object are meaningless and return {@code null}.
     */
    public boolean isDecisionTaken() {
        return finalDecision != null;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the unassigned shard.
     * This value can only be {@code null} if {@link #isDecisionTaken()} returns {@code false}.
     */
    @Nullable
    public Decision getFinalDecision() {
        return finalDecision;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the unassigned shard.
     * Only call this method if {@link #isDecisionTaken()} returns {@code true}, otherwise it will
     * throw an {@code IllegalArgumentException}.
     */
    public Decision getFinalDecisionSafe() {
        if (isDecisionTaken() == false) {
            throw new IllegalArgumentException("decision must have been taken in order to return the final decision");
        }
        return finalDecision;
    }

    /**
     * Returns the status of an unsuccessful allocation attempt.  This value will be {@code null} if
     * no decision was taken or if the decision was {@link Decision.Type#YES}.
     */
    @Nullable
    public AllocationStatus getAllocationStatus() {
        return allocationStatus;
    }

    /**
     * Returns the free-text explanation for the reason behind the decision taken in {@link #getFinalDecision()}.
     */
    @Nullable
    public String getFinalExplanation() {
        return finalExplanation;
    }

    /**
     * Returns the free-text explanation for the reason behind the decision taken in {@link #getFinalDecision()}.
     * Only call this method if {@link #isDecisionTaken()} returns {@code true}, otherwise it will
     * throw an {@code IllegalArgumentException}.
     */
    public String getFinalExplanationSafe() {
        if (isDecisionTaken() == false) {
            throw new IllegalArgumentException("decision must have been taken in order to return the final explanation");
        }
        return finalExplanation;
    }

    /**
     * Get the node id that the allocator will assign the shard to, unless {@link #getFinalDecision()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    /**
     * Gets the allocation id for the existing shard copy that the allocator is assigning the shard to.
     * This method returns a non-null value iff {@link #getAssignedNodeId()} returns a non-null value
     * and the node on which the shard is assigned already has a shard copy with an in-sync allocation id
     * that we can re-use.
     */
    @Nullable
    public String getAllocationId() {
        return allocationId;
    }

    /**
     * Gets the individual node-level decisions that went into making the final decision as represented by
     * {@link #getFinalDecision()}.  The map that is returned has the node id as the key and a {@link Decision}
     * as the decision for the given node.
     */
    @Nullable
    public Map<String, Decision> getNodeDecisions() {
        return nodeDecisions;
    }
}
