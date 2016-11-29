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
 * Represents the allocation decision by an allocator for an unassigned shard.
 */
public class AllocateUnassignedDecision {
    /** a constant representing a shard decision where no decision was taken */
    public static final AllocateUnassignedDecision NOT_TAKEN =
        new AllocateUnassignedDecision(null, null, null, null, null, null, null);
    /**
     * a map of cached common no/throttle decisions that don't need explanations,
     * this helps prevent unnecessary object allocations for the non-explain API case
     */
    private static final Map<AllocationStatus, AllocateUnassignedDecision> CACHED_DECISIONS;
    static {
        Map<AllocationStatus, AllocateUnassignedDecision> cachedDecisions = new HashMap<>();
        cachedDecisions.put(AllocationStatus.FETCHING_SHARD_DATA,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.FETCHING_SHARD_DATA, null, null, null, null, null));
        cachedDecisions.put(AllocationStatus.NO_VALID_SHARD_COPY,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.NO_VALID_SHARD_COPY, null, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DECIDERS_NO,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DECIDERS_NO, null, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DECIDERS_THROTTLED,
            new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null, null, null, null));
        cachedDecisions.put(AllocationStatus.DELAYED_ALLOCATION,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DELAYED_ALLOCATION, null, null, null, null, null));
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
    private final Map<String, NodeAllocationResult> nodeDecisions;
    @Nullable
    private final Decision shardDecision;

    private AllocateUnassignedDecision(Type finalDecision,
                                       AllocationStatus allocationStatus,
                                       String finalExplanation,
                                       String assignedNodeId,
                                       String allocationId,
                                       Map<String, NodeAllocationResult> nodeDecisions,
                                       Decision shardDecision) {
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
        this.shardDecision = shardDecision;
    }

    /**
     * Returns a NO decision with the given shard-level decision and explanation (if in explain mode).
     */
    public static AllocateUnassignedDecision no(Decision shardDecision, @Nullable String explanation) {
        if (explanation != null) {
            return new AllocateUnassignedDecision(Type.NO, AllocationStatus.DECIDERS_NO, explanation, null, null, null, shardDecision);
        } else {
            return getCachedDecision(AllocationStatus.DECIDERS_NO);
        }
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus} and explanation for the NO decision, if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable String explanation) {
        return no(allocationStatus, explanation, null);
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the explanation for the NO decision
     * as well as the individual node-level decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable String explanation,
                                                @Nullable Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(allocationStatus, "allocationStatus must not be null");
        if (explanation != null) {
            return new AllocateUnassignedDecision(Type.NO, allocationStatus, explanation, null, null, asExplanations(nodeDecisions), null);
        } else {
            return getCachedDecision(allocationStatus);
        }
    }

    /**
     * Returns a THROTTLE decision, with the given explanation and individual node-level decisions that
     * comprised the final THROTTLE decision if in explain mode.
     */
    public static AllocateUnassignedDecision throttle(@Nullable String explanation, @Nullable Map<String, Decision> nodeDecisions) {
        if (explanation != null) {
            return new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, explanation, null, null,
                                               asExplanations(nodeDecisions), null);
        } else {
            return getCachedDecision(AllocationStatus.DECIDERS_THROTTLED);
        }
    }

    /**
     * Creates a YES decision with the given explanation and individual node-level decisions that
     * comprised the final YES decision, along with the node id to which the shard is assigned and
     * the allocation id for the shard, if available.
     */
    public static AllocateUnassignedDecision yes(String assignedNodeId, @Nullable String explanation, @Nullable String allocationId,
                                                 @Nullable Map<String, Decision> nodeDecisions) {
        Objects.requireNonNull(assignedNodeId, "assignedNodeId must not be null");
        return new AllocateUnassignedDecision(Type.YES, null, explanation, assignedNodeId, allocationId,
                                                 asExplanations(nodeDecisions), null);
    }

    /**
     * Creates a {@link AllocateUnassignedDecision} from the given {@link Decision} and the assigned node, if any.
     */
    public static AllocateUnassignedDecision fromDecision(Decision decision, @Nullable String assignedNodeId, boolean explain,
                                                          @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
        final Type decisionType = decision.type();
        AllocationStatus allocationStatus = decisionType != Type.YES ? AllocationStatus.fromDecision(decisionType) : null;
        String explanation = null;
        if (explain) {
            if (decision.type() == Type.YES) {
                assert assignedNodeId != null;
                explanation = "shard assigned to node [" + assignedNodeId + "]";
            } else if (decision.type() == Type.THROTTLE) {
                assert assignedNodeId != null;
                explanation = "shard assignment throttled on node [" + assignedNodeId + "]";
            } else {
                explanation = "shard cannot be assigned to any node in the cluster";
            }
        }
        return new AllocateUnassignedDecision(decisionType, allocationStatus, explanation, assignedNodeId, null, nodeDecisions, null);
    }

    private static AllocateUnassignedDecision getCachedDecision(AllocationStatus allocationStatus) {
        AllocateUnassignedDecision decision = CACHED_DECISIONS.get(allocationStatus);
        return Objects.requireNonNull(decision, "precomputed decision not found for " + allocationStatus);
    }

    private static Map<String, NodeAllocationResult> asExplanations(Map<String, Decision> decisionMap) {
        if (decisionMap != null) {
            Map<String, NodeAllocationResult> explanationMap = new HashMap<>();
            for (Map.Entry<String, Decision> entry : decisionMap.entrySet()) {
                explanationMap.put(entry.getKey(), new NodeAllocationResult(entry.getValue(), Float.POSITIVE_INFINITY));
            }
            return explanationMap;
        }
        return null;
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
    public Type getFinalDecisionType() {
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
     * Returns the free-text explanation for the reason behind the decision taken in {@link #getFinalDecisionType()}.
     */
    @Nullable
    public String getFinalExplanation() {
        return finalExplanation;
    }

    /**
     * Get the node id that the allocator will assign the shard to, unless {@link #getFinalDecisionType()} returns
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
     * {@link #getFinalDecisionType()}.  The map that is returned has the node id as the key and a {@link Decision}
     * as the decision for the given node.
     */
    @Nullable
    public Map<String, NodeAllocationResult> getNodeDecisions() {
        return nodeDecisions;
    }

    /**
     * Gets the decision on allocating a shard, without examining any specific nodes to allocate to
     * (e.g. a replica can never be allocated if the primary is not allocated, so this is a shard-level
     * decision, not having taken any node into account).
     */
    @Nullable
    public Decision getShardDecision() {
        return shardDecision;
    }

}
