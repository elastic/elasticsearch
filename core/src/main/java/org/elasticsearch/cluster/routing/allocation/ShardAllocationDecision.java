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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the allocation decision by an allocator for a shard.
 */
public class ShardAllocationDecision {
    /** a constant representing a shard decision where no decision was taken */
    public static final ShardAllocationDecision DECISION_NOT_TAKEN =
        new ShardAllocationDecision(null, null, null, null, null, null);
    /**
     * a map of cached common no/throttle decisions that don't need explanations,
     * this helps prevent unnecessary object allocations for the non-explain API case
     */
    private static final Map<AllocationStatus, ShardAllocationDecision> CACHED_DECISIONS;
    static {
        Map<AllocationStatus, ShardAllocationDecision> cachedDecisions = new HashMap<>();
        cachedDecisions.put(AllocationStatus.FETCHING_SHARD_DATA,
            new ShardAllocationDecision(Type.NO, AllocationStatus.FETCHING_SHARD_DATA, null, null, null, null));
        cachedDecisions.put(AllocationStatus.NO_VALID_SHARD_COPY,
            new ShardAllocationDecision(Type.NO, AllocationStatus.NO_VALID_SHARD_COPY, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DECIDERS_NO,
            new ShardAllocationDecision(Type.NO, AllocationStatus.DECIDERS_NO, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DECIDERS_THROTTLED,
            new ShardAllocationDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DELAYED_ALLOCATION,
            new ShardAllocationDecision(Type.NO, AllocationStatus.DELAYED_ALLOCATION, null, null, null, null));
        CACHED_DECISIONS = Collections.unmodifiableMap(cachedDecisions);
    }

    @Nullable
    private final Type finalDecision;
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

    private ShardAllocationDecision(Type finalDecision,
                                    AllocationStatus allocationStatus,
                                    String finalExplanation,
                                    String assignedNodeId,
                                    String allocationId,
                                    Map<String, Decision> nodeDecisions) {
        assert assignedNodeId != null || finalDecision == null || finalDecision != Type.YES :
            "a yes decision must have a node to assign the shard to";
        assert allocationStatus != null || finalDecision == null || finalDecision == Type.YES :
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
     * Returns a NO decision with the given {@link AllocationStatus} and explanation for the NO decision, if in explain mode.
     */
    public static ShardAllocationDecision no(AllocationStatus allocationStatus, @Nullable String explanation) {
        return no(allocationStatus, explanation, null);
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the explanation for the NO decision
     * as well as the individual node-level decisions that comprised the final NO decision if in explain mode.
     */
    public static ShardAllocationDecision no(AllocationStatus allocationStatus, @Nullable String explanation,
                                             @Nullable Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(allocationStatus, "allocationStatus must not be null");
        if (explanation != null) {
            return new ShardAllocationDecision(Type.NO, allocationStatus, explanation, null, null, nodeDecisions);
        } else {
            return getCachedDecision(allocationStatus);
        }
    }

    /**
     * Returns a THROTTLE decision, with the given explanation and individual node-level decisions that
     * comprised the final THROTTLE decision if in explain mode.
     */
    public static ShardAllocationDecision throttle(@Nullable String explanation, @Nullable Map<String, Decision> nodeDecisions) {
        if (explanation != null) {
            return new ShardAllocationDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, explanation, null, null, nodeDecisions);
        } else {
            return getCachedDecision(AllocationStatus.DECIDERS_THROTTLED);
        }
    }

    /**
     * Creates a YES decision with the given explanation and individual node-level decisions that
     * comprised the final YES decision, along with the node id to which the shard is assigned and
     * the allocation id for the shard, if available.
     */
    public static ShardAllocationDecision yes(String assignedNodeId, @Nullable String explanation, @Nullable String allocationId,
                                              @Nullable Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(assignedNodeId, "assignedNodeId must not be null");
        return new ShardAllocationDecision(Type.YES, null, explanation, assignedNodeId, allocationId, nodeDecisions);
    }

    private static ShardAllocationDecision getCachedDecision(AllocationStatus allocationStatus) {
        ShardAllocationDecision decision = CACHED_DECISIONS.get(allocationStatus);
        return Objects.requireNonNull(decision, "precomputed decision not found for " + allocationStatus);
    }

    /**
     * Returns <code>true</code> if a decision was taken by the allocator, {@code false} otherwise.
     * If no decision was taken, then the rest of the fields in this object are meaningless and return {@code null}.
     */
    public boolean isDecisionTaken() {
        return finalDecision != null;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the shard.
     * This value can only be {@code null} if {@link #isDecisionTaken()} returns {@code false}.
     */
    @Nullable
    public Type getFinalDecision() {
        return finalDecision;
    }

    /**
     * Returns the final decision made by the allocator on whether to assign the shard.
     * Only call this method if {@link #isDecisionTaken()} returns {@code true}, otherwise it will
     * throw an {@code IllegalArgumentException}.
     */
    public Type getFinalDecisionSafe() {
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
