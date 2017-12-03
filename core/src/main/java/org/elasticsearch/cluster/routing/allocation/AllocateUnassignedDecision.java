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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents the allocation decision by an allocator for an unassigned shard.
 */
public class AllocateUnassignedDecision extends AbstractAllocationDecision {
    /** a constant representing a shard decision where no decision was taken */
    public static final AllocateUnassignedDecision NOT_TAKEN =
        new AllocateUnassignedDecision(AllocationStatus.NO_ATTEMPT, null, null, null, false, 0L, 0L);
    /**
     * a map of cached common no/throttle decisions that don't need explanations,
     * this helps prevent unnecessary object allocations for the non-explain API case
     */
    private static final Map<AllocationStatus, AllocateUnassignedDecision> CACHED_DECISIONS;
    static {
        Map<AllocationStatus, AllocateUnassignedDecision> cachedDecisions = new EnumMap<>(AllocationStatus.class);
        cachedDecisions.put(AllocationStatus.FETCHING_SHARD_DATA,
            new AllocateUnassignedDecision(AllocationStatus.FETCHING_SHARD_DATA, null, null, null, false, 0L, 0L));
        cachedDecisions.put(AllocationStatus.NO_VALID_SHARD_COPY,
            new AllocateUnassignedDecision(AllocationStatus.NO_VALID_SHARD_COPY, null, null, null, false, 0L, 0L));
        cachedDecisions.put(AllocationStatus.DECIDERS_NO,
            new AllocateUnassignedDecision(AllocationStatus.DECIDERS_NO, null, null, null, false, 0L, 0L));
        cachedDecisions.put(AllocationStatus.DECIDERS_THROTTLED,
            new AllocateUnassignedDecision(AllocationStatus.DECIDERS_THROTTLED, null, null, null, false, 0L, 0L));
        cachedDecisions.put(AllocationStatus.DELAYED_ALLOCATION,
            new AllocateUnassignedDecision(AllocationStatus.DELAYED_ALLOCATION, null, null, null, false, 0L, 0L));
        CACHED_DECISIONS = Collections.unmodifiableMap(cachedDecisions);
    }

    @Nullable
    private final AllocationStatus allocationStatus;
    @Nullable
    private final String allocationId;
    private final boolean reuseStore;
    private final long remainingDelayInMillis;
    private final long configuredDelayInMillis;

    private AllocateUnassignedDecision(AllocationStatus allocationStatus,
                                       DiscoveryNode assignedNode,
                                       String allocationId,
                                       List<NodeAllocationResult> nodeDecisions,
                                       boolean reuseStore,
                                       long remainingDelayInMillis,
                                       long configuredDelayInMillis) {
        super(assignedNode, nodeDecisions);
        assert assignedNode != null || allocationStatus != null :
            "a yes decision must have a node to assign the shard to";
        assert allocationId == null || assignedNode != null :
            "allocation id can only be null if the assigned node is null";
        this.allocationStatus = allocationStatus;
        this.allocationId = allocationId;
        this.reuseStore = reuseStore;
        this.remainingDelayInMillis = remainingDelayInMillis;
        this.configuredDelayInMillis = configuredDelayInMillis;
    }

    public AllocateUnassignedDecision(StreamInput in) throws IOException {
        super(in);
        allocationStatus = in.readOptionalWriteable(AllocationStatus::readFrom);
        allocationId = in.readOptionalString();
        reuseStore = in.readBoolean();
        remainingDelayInMillis = in.readVLong();
        configuredDelayInMillis = in.readVLong();
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable List<NodeAllocationResult> decisions) {
        return no(allocationStatus, decisions, false);
    }

    /**
     * Returns a NO decision for a delayed shard allocation on a replica shard, with the individual node-level
     * decisions that comprised the final NO decision, if in explain mode.  Instances created with this
     * method will return {@link AllocationStatus#DELAYED_ALLOCATION} for {@link #getAllocationStatus()}.
     */
    public static AllocateUnassignedDecision delayed(long remainingDelay, long totalDelay,
                                                     @Nullable List<NodeAllocationResult> decisions) {
        return no(AllocationStatus.DELAYED_ALLOCATION, decisions, false, remainingDelay, totalDelay);
    }

    /**
     * Returns a NO decision with the given {@link AllocationStatus}, and the individual node-level
     * decisions that comprised the final NO decision if in explain mode.
     */
    public static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable List<NodeAllocationResult> decisions,
                                                boolean reuseStore) {
        return no(allocationStatus, decisions, reuseStore, 0L, 0L);
    }

    private static AllocateUnassignedDecision no(AllocationStatus allocationStatus, @Nullable List<NodeAllocationResult> decisions,
                                                 boolean reuseStore, long remainingDelay, long totalDelay) {
        if (decisions != null) {
            return new AllocateUnassignedDecision(allocationStatus, null, null, decisions, reuseStore, remainingDelay, totalDelay);
        } else {
            return getCachedDecision(allocationStatus);
        }
    }

    /**
     * Returns a THROTTLE decision, with the individual node-level decisions that
     * comprised the final THROTTLE decision if in explain mode.
     */
    public static AllocateUnassignedDecision throttle(@Nullable List<NodeAllocationResult> decisions) {
        if (decisions != null) {
            return new AllocateUnassignedDecision(AllocationStatus.DECIDERS_THROTTLED, null, null, decisions, false, 0L, 0L);
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
                                                 @Nullable List<NodeAllocationResult> decisions, boolean reuseStore) {
        return new AllocateUnassignedDecision(null, assignedNode, allocationId, decisions, reuseStore, 0L, 0L);
    }

    /**
     * Creates a {@link AllocateUnassignedDecision} from the given {@link Decision} and the assigned node, if any.
     */
    public static AllocateUnassignedDecision fromDecision(Decision decision, @Nullable DiscoveryNode assignedNode,
                                                          @Nullable List<NodeAllocationResult> nodeDecisions) {
        final Type decisionType = decision.type();
        AllocationStatus allocationStatus = decisionType != Type.YES ? AllocationStatus.fromDecision(decisionType) : null;
        return new AllocateUnassignedDecision(allocationStatus, assignedNode, null, nodeDecisions, false, 0L, 0L);
    }

    private static AllocateUnassignedDecision getCachedDecision(AllocationStatus allocationStatus) {
        AllocateUnassignedDecision decision = CACHED_DECISIONS.get(allocationStatus);
        return Objects.requireNonNull(decision, "precomputed decision not found for " + allocationStatus);
    }

    @Override
    public boolean isDecisionTaken() {
        return allocationStatus != AllocationStatus.NO_ATTEMPT;
    }

    /**
     * Returns the {@link AllocationDecision} denoting the result of an allocation attempt.
     * If {@link #isDecisionTaken()} returns {@code false}, then invoking this method will
     * throw an {@code IllegalStateException}.
     */
    public AllocationDecision getAllocationDecision() {
        checkDecisionState();
        return AllocationDecision.fromAllocationStatus(allocationStatus);
    }

    /**
     * Returns the status of an unsuccessful allocation attempt.  This value will be {@code null} if
     * no decision was taken or if the decision was {@link Decision.Type#YES}.  If {@link #isDecisionTaken()}
     * returns {@code false}, then invoking this method will throw an {@code IllegalStateException}.
     */
    @Nullable
    public AllocationStatus getAllocationStatus() {
        checkDecisionState();
        return allocationStatus;
    }

    /**
     * Gets the allocation id for the existing shard copy that the allocator is assigning the shard to.
     * This method returns a non-null value iff {@link #getTargetNode()} returns a non-null value
     * and the node on which the shard is assigned already has a shard copy with an in-sync allocation id
     * that we can re-use.  If {@link #isDecisionTaken()} returns {@code false}, then invoking this method
     * will throw an {@code IllegalStateException}.
     */
    @Nullable
    public String getAllocationId() {
        checkDecisionState();
        return allocationId;
    }

    /**
     * Gets the remaining delay for allocating the replica shard when a node holding the replica left
     * the cluster and the deciders are waiting to see if the node returns before allocating the replica
     * elsewhere.  Only returns a meaningful positive value if {@link #getAllocationStatus()} returns
     * {@link AllocationStatus#DELAYED_ALLOCATION}.  If {@link #isDecisionTaken()} returns {@code false},
     * then invoking this method will throw an {@code IllegalStateException}.
     */
    public long getRemainingDelayInMillis() {
        checkDecisionState();
        return remainingDelayInMillis;
    }

    /**
     * Gets the total configured delay for allocating the replica shard when a node holding the replica left
     * the cluster and the deciders are waiting to see if the node returns before allocating the replica
     * elsewhere.  Only returns a meaningful positive value if {@link #getAllocationStatus()} returns
     * {@link AllocationStatus#DELAYED_ALLOCATION}.  If {@link #isDecisionTaken()} returns {@code false},
     * then invoking this method will throw an {@code IllegalStateException}.
     */
    public long getConfiguredDelayInMillis() {
        checkDecisionState();
        return configuredDelayInMillis;
    }

    @Override
    public String getExplanation() {
        checkDecisionState();
        AllocationDecision allocationDecision = getAllocationDecision();
        if (allocationDecision == AllocationDecision.YES) {
            return "can allocate the shard";
        } else if (allocationDecision == AllocationDecision.THROTTLED) {
            return "allocation temporarily throttled";
        } else if (allocationDecision == AllocationDecision.AWAITING_INFO) {
            return "cannot allocate because information about existing shard data is still being retrieved from some of the nodes";
        } else if (allocationDecision == AllocationDecision.NO_VALID_SHARD_COPY) {
            if (hasNodeWithStaleOrCorruptShard()) {
                return "cannot allocate because all found copies of the shard are either stale or corrupt";
            } else {
                return "cannot allocate because a previous copy of the primary shard existed but can no longer be found on " +
                       "the nodes in the cluster";
            }
        } else if (allocationDecision == AllocationDecision.ALLOCATION_DELAYED) {
            return "cannot allocate because the cluster is still waiting " +
                              TimeValue.timeValueMillis(remainingDelayInMillis) +
                              " for the departed node holding a replica to rejoin" +
                              (atLeastOneNodeWithYesDecision() ?
                                   ", despite being allowed to allocate the shard to at least one other node" : "");
        } else {
            assert allocationDecision == AllocationDecision.NO;
            if (reuseStore) {
                return "cannot allocate because allocation is not permitted to any of the nodes that hold an in-sync shard copy";
            } else {
                return "cannot allocate because allocation is not permitted to any of the nodes";
            }
        }
    }

    private boolean hasNodeWithStaleOrCorruptShard() {
        return getNodeDecisions() != null && getNodeDecisions().stream().anyMatch(result ->
                result.getShardStoreInfo() != null
                    && (result.getShardStoreInfo().getAllocationId() != null
                            || result.getShardStoreInfo().getStoreException() != null));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        checkDecisionState();
        builder.field("can_allocate", getAllocationDecision());
        builder.field("allocate_explanation", getExplanation());
        if (targetNode != null) {
            builder.startObject("target_node");
            discoveryNodeToXContent(targetNode, true, builder);
            builder.endObject();
        }
        if (allocationId != null) {
            builder.field("allocation_id", allocationId);
        }
        if (allocationStatus == AllocationStatus.DELAYED_ALLOCATION) {
            builder.timeValueField("configured_delay_in_millis", "configured_delay", TimeValue.timeValueMillis(configuredDelayInMillis));
            builder.timeValueField("remaining_delay_in_millis", "remaining_delay", TimeValue.timeValueMillis(remainingDelayInMillis));
        }
        nodeDecisionsToXContent(nodeDecisions, builder, params);
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(allocationStatus);
        out.writeOptionalString(allocationId);
        out.writeBoolean(reuseStore);
        out.writeVLong(remainingDelayInMillis);
        out.writeVLong(configuredDelayInMillis);
    }

    @Override
    public boolean equals(Object other) {
        if (super.equals(other) == false) {
            return false;
        }
        if (other instanceof AllocateUnassignedDecision == false) {
            return false;
        }
        @SuppressWarnings("unchecked") AllocateUnassignedDecision that = (AllocateUnassignedDecision) other;
        return Objects.equals(allocationStatus, that.allocationStatus)
                   && Objects.equals(allocationId, that.allocationId)
                   && reuseStore == that.reuseStore
                   && configuredDelayInMillis == that.configuredDelayInMillis
                   && remainingDelayInMillis == that.remainingDelayInMillis;
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(allocationStatus, allocationId, reuseStore,
            configuredDelayInMillis, remainingDelayInMillis);
    }

}
