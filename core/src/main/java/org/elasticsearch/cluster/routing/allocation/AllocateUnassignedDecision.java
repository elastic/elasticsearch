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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the allocation decision by an allocator for an unassigned shard.
 */
public class AllocateUnassignedDecision implements ToXContent, Writeable {
    /** a constant representing a shard decision where no decision was taken */
    public static final AllocateUnassignedDecision NOT_TAKEN =
        new AllocateUnassignedDecision(null, null, null, null, null, false, false);
    /**
     * a map of cached common no/throttle decisions that don't need explanations,
     * this helps prevent unnecessary object allocations for the non-explain API case
     */
    private static final Map<AllocationStatus, AllocateUnassignedDecision> CACHED_DECISIONS;
    static {
        Map<AllocationStatus, AllocateUnassignedDecision> cachedDecisions = new HashMap<>();
        cachedDecisions.put(AllocationStatus.FETCHING_SHARD_DATA,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.FETCHING_SHARD_DATA, null, null, null, false, false));
        cachedDecisions.put(AllocationStatus.NO_VALID_SHARD_COPY,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.NO_VALID_SHARD_COPY, null, null, null, false, false));
        cachedDecisions.put(AllocationStatus.DECIDERS_NO,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DECIDERS_NO, null, null, null, false, false));
        cachedDecisions.put(AllocationStatus.DECIDERS_THROTTLED,
            new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null, null, false, false));
        cachedDecisions.put(AllocationStatus.DELAYED_ALLOCATION,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DELAYED_ALLOCATION, null, null, null, false, false));
        CACHED_DECISIONS = Collections.unmodifiableMap(cachedDecisions);
    }

    @Nullable
    private final Type finalDecision;
    @Nullable
    private final AllocationStatus allocationStatus;
    @Nullable
    private final String assignedNodeId;
    @Nullable
    private final String allocationId;
    @Nullable
    private final Map<String, NodeAllocationResult> nodeDecisions;
    private final boolean forceAllocated;
    private final boolean reuseStore;

    private AllocateUnassignedDecision(Type finalDecision,
                                       AllocationStatus allocationStatus,
                                       String assignedNodeId,
                                       String allocationId,
                                       Map<String, NodeAllocationResult> nodeDecisions,
                                       boolean forceAllocated,
                                       boolean reuseStore) {
        assert assignedNodeId != null || finalDecision == null || finalDecision != Type.YES :
            "a yes decision must have a node to assign the shard to";
        assert allocationStatus != null || finalDecision == null || finalDecision == Type.YES :
            "only a yes decision should not have an allocation status";
        assert allocationId == null || assignedNodeId != null :
            "allocation id can only be null if the assigned node is null";
        this.finalDecision = finalDecision;
        this.allocationStatus = allocationStatus;
        this.assignedNodeId = assignedNodeId;
        this.allocationId = allocationId;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
        this.forceAllocated = forceAllocated;
        this.reuseStore = reuseStore;
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable Map<String, NodeAllocationResult> decisions) {
        return no(allocationStatus, decisions, false);
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable Map<String, NodeAllocationResult> decisions,
                                                boolean reuseStore) {
        if (decisions != null) {
            return new AllocateUnassignedDecision(Type.NO, allocationStatus, null, null, decisions, false, reuseStore);
        } else {
            return getCachedDecision(allocationStatus);
        }
    }

    /**
     * Returns a THROTTLE decision, with the individual node-level decisions that
     * comprised the final THROTTLE decision if in explain mode.
     */
    public static AllocateUnassignedDecision throttle(@Nullable Map<String, NodeAllocationResult> decisions) {
        if (decisions != null) {
            return new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null,
                                                     decisions, false, false);
        } else {
            return getCachedDecision(AllocationStatus.DECIDERS_THROTTLED);
        }
    }

    /**
     * Creates a YES decision with the given individual node-level decisions that
     * comprised the final YES decision, along with the node id to which the shard is assigned and
     * the allocation id for the shard, if available.
     */
    public static AllocateUnassignedDecision yes(String assignedNodeId, @Nullable String allocationId,
                                                 @Nullable Map<String, NodeAllocationResult> decisions, boolean forceAllocated,
                                                 boolean reuseStore) {
        return new AllocateUnassignedDecision(Type.YES, null, assignedNodeId, allocationId, decisions, forceAllocated, reuseStore);
    }

    /**
     * Creates a {@link AllocateUnassignedDecision} from the given {@link Decision} and the assigned node, if any.
     */
    public static AllocateUnassignedDecision fromDecision(Decision decision, @Nullable String assignedNodeId,
                                                          @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
        final Type decisionType = decision.type();
        AllocationStatus allocationStatus = decisionType != Type.YES ? AllocationStatus.fromDecision(decisionType) : null;
        return new AllocateUnassignedDecision(decisionType, allocationStatus, assignedNodeId, null, nodeDecisions, false, false);
    }

    private static AllocateUnassignedDecision getCachedDecision(AllocationStatus allocationStatus) {
        AllocateUnassignedDecision decision = CACHED_DECISIONS.get(allocationStatus);
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
     * Returns {@code true} if the deciders returned a NO decision, but because the shard is a primary
     * and a prior copy of it exists, the allocators allocated the primary to a node that has a copy
     * of the shard, despite the NO decision from the deciders.
     */
    public boolean isForceAllocated() {
        return forceAllocated;
    }

    /**
     * Returns the explanation behind the {@link #getFinalDecisionType()} that is returned for this decision.
     */
    public String getFinalExplanation() {
        String explanation;
        if (finalDecision == Type.NO) {
            assert allocationStatus != null : "if the decision is NO, it must have an AllocationStatus";
            if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
                explanation = "still fetching shard state from the nodes in the cluster";
            } else if (allocationStatus == AllocationStatus.NO_VALID_SHARD_COPY) {
                explanation = "shard was previously allocated, but no valid shard copy could be found amongst the nodes in the cluster";
            } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
                explanation = "delaying allocation of the replica because there are no other shard copies to use; waiting to see if " +
                                  "the node that held the replica rejoins the cluster";
            } else {
                assert allocationStatus == AllocationStatus.DECIDERS_NO;
                if (reuseStore) {
                    explanation = "all nodes that hold a valid shard copy returned a NO decision";
                } else {
                    explanation = "shard cannot be assigned to any node in the cluster";
                }
            }
        } else if (finalDecision == Type.YES) {
            if (forceAllocated) {
                explanation = "allocating the primary shard to node [" + assignedNodeId + "], which has a complete copy of " +
                                  "the shard data with allocationId [" + allocationId + "]";
            } else if (allocationId != null) {
                explanation = "the allocation deciders returned a YES decision to allocate to node [" + assignedNodeId + "] " +
                                  "with allocation id [" + allocationId + "]";
            } else if (reuseStore) {
                explanation = "allocating replica to node [" + assignedNodeId + "] in order to re-use its unallocated persistent store";
            } else {
                explanation = "shard assigned to node [" + assignedNodeId + "]";
            }
        } else if (finalDecision == Type.THROTTLE) {
            explanation = "allocation throttled as each node with an existing copy of the shard is busy with other recoveries";
        } else {
            explanation = "decision not taken";
        }
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("final_decision", finalDecision.toString());
        builder.field("final_explanation", getFinalExplanation());
        if (allocationStatus != null) {
            builder.field("allocation_status", allocationStatus.value());
        }
        if (assignedNodeId != null) {
            builder.field("assigned_node_id", assignedNodeId);
        }
        if (allocationId != null) {
            builder.field("allocation_id", allocationId);
        }
        if (nodeDecisions != null) {
            builder.startObject("nodes");
            {
                List<String> nodeIds = new ArrayList<>(nodeDecisions.keySet());
                Collections.sort(nodeIds);
                for (String nodeId : nodeIds) {
                    NodeAllocationResult nodeAllocationResult = nodeDecisions.get(nodeId);
                    nodeAllocationResult.toXContent(builder, params);
                }
            }
            builder.endObject();
        }
        return builder;
    }

    public static AllocateUnassignedDecision readFrom(StreamInput in) throws IOException {
        Type finalDecision;
        if (in.readBoolean()) {
            finalDecision = Type.readFrom(in);
        } else {
            return NOT_TAKEN;
        }
        AllocationStatus allocationStatus = null;
        if (in.readBoolean()) {
            allocationStatus = AllocationStatus.readFrom(in);
        }
        String assignedNodeId = in.readOptionalString();
        String allocationId = in.readOptionalString();

        Map<String, NodeAllocationResult> nodeDecisions = null;
        if (in.readBoolean()) {
            final int size = in.readVInt();
            nodeDecisions = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                nodeDecisions.put(in.readString(), new NodeAllocationResult(in));
            }
        }

        if (nodeDecisions == null && allocationStatus != null) {
            // use cached version - there are no detailed decisions or explanations
            return CACHED_DECISIONS.get(allocationStatus);
        }

        boolean forceAllocated = in.readBoolean();
        boolean reuseStore = in.readBoolean();

        return new AllocateUnassignedDecision(finalDecision, allocationStatus, assignedNodeId, allocationId,
                                                 nodeDecisions, forceAllocated, reuseStore);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (finalDecision != null) {
            out.writeBoolean(true);
            Type.writeTo(finalDecision, out);
        } else {
            out.writeBoolean(false);
        }
        if (allocationStatus != null) {
            out.writeBoolean(true);
            allocationStatus.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(assignedNodeId);
        out.writeOptionalString(allocationId);
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
        out.writeBoolean(forceAllocated);
        out.writeBoolean(reuseStore);
    }

}
