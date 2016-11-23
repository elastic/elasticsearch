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
        new AllocateUnassignedDecision(null, null, null, null, null, false, 0);
    /**
     * a map of cached common no/throttle decisions that don't need explanations,
     * this helps prevent unnecessary object allocations for the non-explain API case
     */
    private static final Map<AllocationStatus, AllocateUnassignedDecision> CACHED_DECISIONS;
    static {
        Map<AllocationStatus, AllocateUnassignedDecision> cachedDecisions = new HashMap<>();
        cachedDecisions.put(AllocationStatus.FETCHING_SHARD_DATA,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.FETCHING_SHARD_DATA, null, null, null, false, 0));
        cachedDecisions.put(AllocationStatus.NO_VALID_SHARD_COPY,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.NO_VALID_SHARD_COPY, null, null, null, false, 0));
        cachedDecisions.put(AllocationStatus.DECIDERS_NO,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DECIDERS_NO, null, null, null, false, 0));
        cachedDecisions.put(AllocationStatus.DECIDERS_THROTTLED,
            new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null, null, false, 0));
        cachedDecisions.put(AllocationStatus.DELAYED_ALLOCATION,
            new AllocateUnassignedDecision(Type.NO, AllocationStatus.DELAYED_ALLOCATION, null, null, null, false, 0));
        CACHED_DECISIONS = Collections.unmodifiableMap(cachedDecisions);
    }

    @Nullable
    private final Type decision;
    @Nullable
    private final AllocationStatus allocationStatus;
    @Nullable
    private final String assignedNodeId;
    @Nullable
    private final String assignedNodeName;
    @Nullable
    private final String allocationId;
    @Nullable
    private final Map<String, NodeAllocationResult> nodeDecisions;
    private final boolean reuseStore;
    private final long remainingDelayInMillis;

    private AllocateUnassignedDecision(Type decision,
                                       AllocationStatus allocationStatus,
                                       DiscoveryNode assignedNode,
                                       String allocationId,
                                       Map<String, NodeAllocationResult> nodeDecisions,
                                       boolean reuseStore,
                                       long remainingDelayInMillis) {
        assert assignedNode != null || decision == null || decision != Type.YES :
            "a yes decision must have a node to assign the shard to";
        assert allocationStatus != null || decision == null || decision == Type.YES :
            "only a yes decision should not have an allocation status";
        assert allocationId == null || assignedNode != null :
            "allocation id can only be null if the assigned node is null";
        this.decision = decision;
        this.allocationStatus = allocationStatus;
        if (assignedNode != null) {
            this.assignedNodeId = assignedNode.getId();
            this.assignedNodeName = assignedNode.getName();
        } else {
            this.assignedNodeId = null;
            this.assignedNodeName = null;
        }
        this.allocationId = allocationId;
        this.nodeDecisions = nodeDecisions != null ? Collections.unmodifiableMap(nodeDecisions) : null;
        this.reuseStore = reuseStore;
        this.remainingDelayInMillis = remainingDelayInMillis;
    }

    public AllocateUnassignedDecision(StreamInput in) throws IOException {
        decision = in.readOptionalWriteable(Type::readFrom);
        allocationStatus = in.readOptionalWriteable(AllocationStatus::readFrom);
        assignedNodeId = in.readOptionalString();
        assignedNodeName = in.readOptionalString();
        allocationId = in.readOptionalString();

        Map<String, NodeAllocationResult> nodeDecisions = null;
        if (in.readBoolean()) {
            final int size = in.readVInt();
            nodeDecisions = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                nodeDecisions.put(in.readString(), new NodeAllocationResult(in));
            }
        }
        this.nodeDecisions = (nodeDecisions != null) ? Collections.unmodifiableMap(nodeDecisions) : null;

        reuseStore = in.readBoolean();
        remainingDelayInMillis = in.readVLong();
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable Map<String, NodeAllocationResult> decisions) {
        return no(allocationStatus, decisions, false);
    }

    /**
     * Returns a NO decision for a delayed shard allocation on a replica shard, with the individual node-level
     * decisions that comprised the final NO decision, if in explain mode.  Instances created with this
     * method will return {@link AllocationStatus#DELAYED_ALLOCATION} for {@link #getAllocationStatus()}.
     */
    public static AllocateUnassignedDecision delayed(long remainingDelay, @Nullable Map<String, NodeAllocationResult> decisions) {
        return no(AllocationStatus.DELAYED_ALLOCATION, decisions, false, remainingDelay);
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable Map<String, NodeAllocationResult> decisions,
                                                boolean reuseStore) {
        return no(allocationStatus, decisions, reuseStore, 0L);
    }

    private static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable Map<String, NodeAllocationResult> decisions,
                                                 boolean reuseStore, long remainingDelay) {
        if (decisions != null) {
            return new AllocateUnassignedDecision(Type.NO, allocationStatus, null, null, decisions, reuseStore, remainingDelay);
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
            return new AllocateUnassignedDecision(Type.THROTTLE, AllocationStatus.DECIDERS_THROTTLED, null, null, decisions, false, 0);
        } else {
            return getCachedDecision(AllocationStatus.DECIDERS_THROTTLED);
        }
    }

    /**
     * Creates a YES decision with the given individual node-level decisions that
     * comprised the final YES decision, along with the node id to which the shard is assigned and
     * the allocation id for the shard, if available.
     */
    public static AllocateUnassignedDecision yes(DiscoveryNode assignedNode, @Nullable String allocationId,
                                                 @Nullable Map<String, NodeAllocationResult> decisions, boolean reuseStore) {
        return new AllocateUnassignedDecision(Type.YES, null, assignedNode, allocationId, decisions, reuseStore, 0);
    }

    /**
     * Creates a {@link AllocateUnassignedDecision} from the given {@link Decision} and the assigned node, if any.
     */
    public static AllocateUnassignedDecision fromDecision(Decision decision, @Nullable DiscoveryNode assignedNode,
                                                          @Nullable Map<String, NodeAllocationResult> nodeDecisions) {
        final Type decisionType = decision.type();
        AllocationStatus allocationStatus = decisionType != Type.YES ? AllocationStatus.fromDecision(decisionType) : null;
        return new AllocateUnassignedDecision(decisionType, allocationStatus, assignedNode, null, nodeDecisions, false, 0L);
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
        return decision != null;
    }

    /**
     * Returns the decision made by the allocator on whether to assign the shard.
     * This value can only be {@code null} if {@link #isDecisionTaken()} returns {@code false}.
     */
    @Nullable
    public Type getDecision() {
        return decision;
    }

    /**
     * Returns the decision made by the allocator on whether to assign the shard.
     * Only call this method if {@link #isDecisionTaken()} returns {@code true}, otherwise it will
     * throw an {@code IllegalArgumentException}.
     */
    public Type getDecisionSafe() {
        if (isDecisionTaken() == false) {
            throw new IllegalArgumentException("decision must have been taken in order to return the final decision");
        }
        return decision;
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
     * Get the node id that the allocator will assign the shard to, unless {@link #getDecision()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    /**
     * Get the node name that the allocator will assign the shard to, unless {@link #getDecision()} returns
     * a value other than {@link Decision.Type#YES}, in which case this returns {@code null}.
     */
    @Nullable
    public String getAssignedNodeName() {
        return assignedNodeName;
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
     * {@link #getDecision()}.  The map that is returned has the node id as the key and a {@link Decision}
     * as the decision for the given node.
     */
    @Nullable
    public Map<String, NodeAllocationResult> getNodeDecisions() {
        return nodeDecisions;
    }

    /**
     * Gets the remaining delay for allocating the replica shard when a node holding the replica left
     * the cluster and the deciders are waiting to see if the node returns before allocating the replica
     * elsewhere.  Only returns a meaningful positive value if {@link #getAllocationStatus()} returns
     * {@link AllocationStatus#DELAYED_ALLOCATION}.
     */
    public long getRemainingDelayInMillis() {
        return remainingDelayInMillis;
    }

    /**
     * Returns the explanation behind the {@link #getDecision()} that is returned for this decision.
     */
    public String getExplanation() {
        String explanation;
        if (decision == Type.NO) {
            assert allocationStatus != null : "if the decision is NO, it must have an AllocationStatus";
            if (allocationStatus == AllocationStatus.FETCHING_SHARD_DATA) {
                explanation = "cannot allocate because information about existing shard data is still being retrieved from " +
                                  "some of the nodes";
            } else if (allocationStatus == AllocationStatus.NO_VALID_SHARD_COPY) {
                if (nodeDecisions != null && nodeDecisions.size() > 0) {
                    explanation = "cannot allocate because all existing copies of the shard are unreadable";
                } else {
                    explanation = "cannot allocate because a previous copy of the shard existed, but could not be found";
                }
            } else if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
                explanation = "cannot allocate because the cluster is waiting " + Long.toString(remainingDelayInMillis / 1000L) +
                                  " seconds for the departed node holding a replica to rejoin";
            } else {
                assert allocationStatus == AllocationStatus.DECIDERS_NO;
                if (reuseStore) {
                    explanation = "cannot allocate because allocation is not permitted to any of the nodes that hold a valid shard copy";
                } else {
                    explanation = "cannot allocate because allocation is not permitted to any of the nodes";
                }
            }
        } else if (decision == Type.YES) {
            explanation = "can allocate the shard";
        } else if (decision == Type.THROTTLE) {
            explanation = "allocation temporarily throttled";
        } else {
            throw new IllegalStateException("unhandled decision [" + decision + "]");
        }
        return explanation;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("decision", decision.toString());
        builder.field("explanation", getExplanation());
        if (allocationStatus != null) {
            builder.field("allocation_status", allocationStatus.value());
        }
        if (assignedNodeId != null) {
            builder.startObject("assigned_node");
            builder.field("id", assignedNodeId);
            builder.field("name", assignedNodeName);
            builder.endObject();
        }
        if (allocationId != null) {
            builder.field("allocation_id", allocationId);
        }
        if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            builder.field("remaining_delay_in_millis", remainingDelayInMillis);
        }
        if (nodeDecisions != null) {
            builder.startObject("node_decisions");
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

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (decision != null) {
            out.writeBoolean(true);
            decision.writeTo(out);
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
        out.writeBoolean(reuseStore);
        out.writeVLong(remainingDelayInMillis);
    }

}
