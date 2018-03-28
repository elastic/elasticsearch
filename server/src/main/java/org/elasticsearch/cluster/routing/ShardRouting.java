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

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent.Params;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * routings like id, state, version, etc.
 */
public final class ShardRouting implements Writeable, ToXContentObject {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;

    private final ShardId shardId;
    private final String currentNodeId;
    private final String relocatingNodeId;
    private final boolean primary;
    private final ShardRoutingState state;
    private final RecoverySource recoverySource;
    private final UnassignedInfo unassignedInfo;
    private final AllocationId allocationId;
    private final transient List<ShardRouting> asList;
    private final long expectedShardSize;
    @Nullable
    private final ShardRouting targetRelocatingShard;

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    ShardRouting(ShardId shardId, String currentNodeId,
                 String relocatingNodeId, boolean primary, ShardRoutingState state, RecoverySource recoverySource,
                 UnassignedInfo unassignedInfo, AllocationId allocationId, long expectedShardSize) {
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.state = state;
        this.recoverySource = recoverySource;
        this.unassignedInfo = unassignedInfo;
        this.allocationId = allocationId;
        this.expectedShardSize = expectedShardSize;
        this.targetRelocatingShard = initializeTargetRelocatingShard();
        this.asList = Collections.singletonList(this);
        assert expectedShardSize == UNAVAILABLE_EXPECTED_SHARD_SIZE || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.RELOCATING : expectedShardSize + " state: " + state;
        assert expectedShardSize >= 0 || state != ShardRoutingState.INITIALIZING || state != ShardRoutingState.RELOCATING : expectedShardSize + " state: " + state;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
        assert (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) == (recoverySource != null) : "recovery source only available on unassigned or initializing shard but was " + state;
        assert recoverySource == null || recoverySource == PeerRecoverySource.INSTANCE || primary : "replica shards always recover from primary";
        assert (currentNodeId == null) == (state == ShardRoutingState.UNASSIGNED)  : "unassigned shard must not be assigned to a node " + this;
    }

    @Nullable
    private ShardRouting initializeTargetRelocatingShard() {
        if (state == ShardRoutingState.RELOCATING) {
            return new ShardRouting(shardId, relocatingNodeId, currentNodeId, primary, ShardRoutingState.INITIALIZING,
                PeerRecoverySource.INSTANCE, unassignedInfo, AllocationId.newTargetRelocation(allocationId), expectedShardSize);
        } else {
            return null;
        }
    }

    /**
     * Creates a new unassigned shard.
     */
    public static ShardRouting newUnassigned(ShardId shardId, boolean primary, RecoverySource recoverySource, UnassignedInfo unassignedInfo) {
        return new ShardRouting(shardId, null, null, primary, ShardRoutingState.UNASSIGNED, recoverySource, unassignedInfo, null, UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public Index index() {
        return shardId.getIndex();
    }

    /**
     * The index name.
     */
    public String getIndexName() {
        return shardId.getIndexName();
    }

    /**
     * The shard id.
     */
    public int id() {
        return shardId.id();
    }

    /**
     * The shard id.
     */
    public int getId() {
        return id();
    }


    /**
     * The shard is unassigned (not allocated to any node).
     */
    public boolean unassigned() {
        return state == ShardRoutingState.UNASSIGNED;
    }

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    public boolean initializing() {
        return state == ShardRoutingState.INITIALIZING;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently
     * {@link ShardRoutingState#STARTED started} or
     * {@link ShardRoutingState#RELOCATING relocating} to another node.
     * Otherwise <code>false</code>
     */
    public boolean active() {
        return started() || relocating();
    }

    /**
     * The shard is in started mode.
     */
    public boolean started() {
        return state == ShardRoutingState.STARTED;
    }

    /**
     * Returns <code>true</code> iff the this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    public boolean relocating() {
        return state == ShardRoutingState.RELOCATING;
    }

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    public boolean assignedToNode() {
        return currentNodeId != null;
    }

    /**
     * The current node id the shard is allocated on.
     */
    public String currentNodeId() {
        return this.currentNodeId;
    }

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    public String relocatingNodeId() {
        return this.relocatingNodeId;
    }

    /**
     * Returns a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting getTargetRelocatingShard() {
        assert relocating();
        return targetRelocatingShard;
    }

    /**
     * Additional metadata on why the shard is/was unassigned. The metadata is kept around
     * until the shard moves to STARTED.
     */
    @Nullable
    public UnassignedInfo unassignedInfo() {
        return unassignedInfo;
    }

    /**
     * An id that uniquely identifies an allocation.
     */
    @Nullable
    public AllocationId allocationId() {
        return this.allocationId;
    }

    /**
     * Returns <code>true</code> iff this shard is a primary.
     */
    public boolean primary() {
        return this.primary;
    }

    /**
     * The shard state.
     */
    public ShardRoutingState state() {
        return this.state;
    }

    /**
     * The shard id.
     */
    public ShardId shardId() {
        return shardId;
    }

    /**
     * A shard iterator with just this shard in it.
     */
    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId, asList);
    }

    public ShardRouting(ShardId shardId, StreamInput in) throws IOException {
        this.shardId = shardId;
        currentNodeId = in.readOptionalString();
        relocatingNodeId = in.readOptionalString();
        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource = RecoverySource.readFrom(in);
        } else {
            recoverySource = null;
        }
        unassignedInfo = in.readOptionalWriteable(UnassignedInfo::new);
        allocationId = in.readOptionalWriteable(AllocationId::new);
        final long shardSize;
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
            shardSize = in.readLong();
        } else {
            shardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
        expectedShardSize = shardSize;
        asList = Collections.singletonList(this);
        targetRelocatingShard = initializeTargetRelocatingShard();
    }

    public ShardRouting(StreamInput in) throws IOException {
        this(ShardId.readShardId(in), in);
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeOptionalString(currentNodeId);
        out.writeOptionalString(relocatingNodeId);
        out.writeBoolean(primary);
        out.writeByte(state.value());
        if (state == ShardRoutingState.UNASSIGNED || state == ShardRoutingState.INITIALIZING) {
            recoverySource.writeTo(out);
        }
        out.writeOptionalWriteable(unassignedInfo);
        out.writeOptionalWriteable(allocationId);
        if (state == ShardRoutingState.RELOCATING || state == ShardRoutingState.INITIALIZING) {
            out.writeLong(expectedShardSize);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        writeToThin(out);
    }

    public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource) {
        assert this.unassignedInfo != null : "can only update unassign info if they are already set";
        assert this.unassignedInfo.isDelayed() || (unassignedInfo.isDelayed() == false) : "cannot transition from non-delayed to delayed";
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, state, recoverySource,
            unassignedInfo, allocationId, expectedShardSize);
    }

    /**
     * Moves the shard to unassigned state.
     */
    public ShardRouting moveToUnassigned(UnassignedInfo unassignedInfo) {
        assert state != ShardRoutingState.UNASSIGNED : this;
        final RecoverySource recoverySource;
        if (active()) {
            if (primary()) {
                recoverySource = StoreRecoverySource.EXISTING_STORE_INSTANCE;
            } else {
                recoverySource = PeerRecoverySource.INSTANCE;
            }
        } else {
            recoverySource = recoverySource();
        }
        return new ShardRouting(shardId, null, null, primary, ShardRoutingState.UNASSIGNED, recoverySource,
            unassignedInfo, null, UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Initializes an unassigned shard on a node.
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     */
    public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize) {
        assert state == ShardRoutingState.UNASSIGNED : this;
        assert relocatingNodeId == null : this;
        final AllocationId allocationId;
        if (existingAllocationId == null) {
            allocationId = AllocationId.newInitializing();
        } else {
            allocationId = AllocationId.newInitializing(existingAllocationId);
        }
        return new ShardRouting(shardId, nodeId, null, primary, ShardRoutingState.INITIALIZING, recoverySource,
            unassignedInfo, allocationId, expectedShardSize);
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard
     */
    public ShardRouting relocate(String relocatingNodeId, long expectedShardSize) {
        assert state == ShardRoutingState.STARTED : "current shard has to be started in order to be relocated " + this;
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, primary, ShardRoutingState.RELOCATING, recoverySource,
            null, AllocationId.newRelocation(allocationId), expectedShardSize);
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     */
    public ShardRouting cancelRelocation() {
        assert state == ShardRoutingState.RELOCATING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.STARTED, recoverySource,
            null, AllocationId.cancelRelocation(allocationId), UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Removes relocation source of a non-primary shard. The shard state must be <code>INITIALIZING</code>.
     * This allows the non-primary shard to continue recovery from the primary even though its non-primary
     * relocation source has failed.
     */
    public ShardRouting removeRelocationSource() {
        assert primary == false : this;
        assert state == ShardRoutingState.INITIALIZING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, state, recoverySource, unassignedInfo,
            AllocationId.finishRelocation(allocationId), expectedShardSize);
    }

    /**
     * Reinitializes a replica shard, giving it a fresh allocation id
     */
    public ShardRouting reinitializeReplicaShard() {
        assert state == ShardRoutingState.INITIALIZING : this;
        assert primary == false : this;
        assert isRelocationTarget() == false : this;
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.INITIALIZING,
            recoverySource, unassignedInfo, AllocationId.newInitializing(), expectedShardSize);
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
     * canceled.
     */
    public ShardRouting moveToStarted() {
        assert state == ShardRoutingState.INITIALIZING : "expected an initializing shard " + this;
        AllocationId allocationId = this.allocationId;
        if (allocationId.getRelocationId() != null) {
            // relocation target
            allocationId = AllocationId.finishRelocation(allocationId);
        }
        return new ShardRouting(shardId, currentNodeId, null, primary, ShardRoutingState.STARTED, null, null, allocationId,
            UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * Make the active shard primary unless it's not primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a primary
     */
    public ShardRouting moveActiveReplicaToPrimary() {
        assert active(): "expected an active shard " + this;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, true, state, recoverySource, unassignedInfo, allocationId,
            expectedShardSize);
    }

    /**
     * Set the unassigned primary shard to non-primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a replica
     */
    public ShardRouting moveUnassignedFromPrimary() {
        assert state == ShardRoutingState.UNASSIGNED : "expected an unassigned shard " + this;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        return new ShardRouting(shardId, currentNodeId, relocatingNodeId, false, state, PeerRecoverySource.INSTANCE, unassignedInfo,
            allocationId, expectedShardSize);
    }

    /**
     * returns true if this routing has the same allocation ID as another.
     * <p>
     * Note: if both shard routing has a null as their {@link #allocationId()}, this method returns false as the routing describe
     * no allocation at all..
     **/
    public boolean isSameAllocation(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && this.allocationId.getId().equals(other.allocationId.getId());
        assert b == false || this.currentNodeId.equals(other.currentNodeId) : "ShardRoutings have the same allocation id but not the same node. This [" + this + "], other [" + other + "]";
        return b;
    }

    /**
     * Returns <code>true</code> if this shard is a relocation target for another shard (i.e., was created with {@link #initializeTargetRelocatingShard()}
     */
    public boolean isRelocationTarget() {
        return state == ShardRoutingState.INITIALIZING && relocatingNodeId != null;
    }

    /** returns true if the routing is the relocation target of the given routing */
    public boolean isRelocationTargetOf(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && this.state == ShardRoutingState.INITIALIZING &&
            this.allocationId.getId().equals(other.allocationId.getRelocationId());

        assert b == false || other.state == ShardRoutingState.RELOCATING :
            "ShardRouting is a relocation target but the source shard state isn't relocating. This [" + this + "], other [" + other + "]";


        assert b == false || other.allocationId.getId().equals(this.allocationId.getRelocationId()) :
            "ShardRouting is a relocation target but the source id isn't equal to source's allocationId.getRelocationId. This [" + this + "], other [" + other + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId) :
            "ShardRouting is a relocation target but source current node id isn't equal to target relocating node. This [" + this + "], other [" + other + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId) :
            "ShardRouting is a relocation target but current node id isn't equal to source relocating node. This [" + this + "], other [" + other + "]";

        assert b == false || this.shardId.equals(other.shardId) :
            "ShardRouting is a relocation target but both routings are not of the same shard id. This [" + this + "], other [" + other + "]";

        assert b == false || this.primary == other.primary :
            "ShardRouting is a relocation target but primary flag is different. This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the routing is the relocation source for the given routing */
    public boolean isRelocationSourceOf(ShardRouting other) {
        boolean b = this.allocationId != null && other.allocationId != null && other.state == ShardRoutingState.INITIALIZING &&
            other.allocationId.getId().equals(this.allocationId.getRelocationId());

        assert b == false || this.state == ShardRoutingState.RELOCATING :
            "ShardRouting is a relocation source but shard state isn't relocating. This [" + this + "], other [" + other + "]";


        assert b == false || this.allocationId.getId().equals(other.allocationId.getRelocationId()) :
            "ShardRouting is a relocation source but the allocation id isn't equal to other.allocationId.getRelocationId. This [" + this + "], other [" + other + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId) :
            "ShardRouting is a relocation source but current node isn't equal to other's relocating node. This [" + this + "], other [" + other + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId) :
            "ShardRouting is a relocation source but relocating node isn't equal to other's current node. This [" + this + "], other [" + other + "]";

        assert b == false || this.shardId.equals(other.shardId) :
            "ShardRouting is a relocation source but both routings are not of the same shard. This [" + this + "], target [" + other + "]";

        assert b == false || this.primary == other.primary :
            "ShardRouting is a relocation source but primary flag is different. This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., unassigned info */
    public boolean equalsIgnoringMetaData(ShardRouting other) {
        if (primary != other.primary) {
            return false;
        }
        if (shardId != null ? !shardId.equals(other.shardId) : other.shardId != null) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(other.currentNodeId) : other.currentNodeId != null) {
            return false;
        }
        if (relocatingNodeId != null ? !relocatingNodeId.equals(other.relocatingNodeId) : other.relocatingNodeId != null) {
            return false;
        }
        if (allocationId != null ? !allocationId.equals(other.allocationId) : other.allocationId != null) {
            return false;
        }
        if (state != other.state) {
            return false;
        }
        if (recoverySource != null ? !recoverySource.equals(other.recoverySource) : other.recoverySource != null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        if (unassignedInfo != null ? !unassignedInfo.equals(that.unassignedInfo) : that.unassignedInfo != null) {
            return false;
        }
        return equalsIgnoringMetaData(that);
    }

    /**
     * Cache hash code in same same way as {@link String#hashCode()}) using racy single-check idiom
     * as it is mainly used in single-threaded code ({@link BalancedShardsAllocator}).
     */
    private int hashCode; // default to 0

    @Override
    public int hashCode() {
        int h = hashCode;
        if (h == 0) {
            h = shardId.hashCode();
            h = 31 * h + (currentNodeId != null ? currentNodeId.hashCode() : 0);
            h = 31 * h + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
            h = 31 * h + (primary ? 1 : 0);
            h = 31 * h + (state != null ? state.hashCode() : 0);
            h = 31 * h + (recoverySource != null ? recoverySource.hashCode() : 0);
            h = 31 * h + (allocationId != null ? allocationId.hashCode() : 0);
            h = 31 * h + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
            hashCode = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return shortSummary();
    }

    /**
     * A short description of the shard.
     */
    public String shortSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append('[').append(shardId.getIndexName()).append(']').append('[').append(shardId.getId()).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (recoverySource != null) {
            sb.append(", recovery_source[").append(recoverySource).append("]");
        }
        sb.append(", s[").append(state).append("]");
        if (allocationId != null) {
            sb.append(", a").append(allocationId);
        }
        if (this.unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo.toString());
        }
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            sb.append(", expected_shard_size[").append(expectedShardSize).append("]");
        }
        return sb.toString();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject()
            .field("state", state())
            .field("primary", primary())
            .field("node", currentNodeId())
            .field("relocating_node", relocatingNodeId())
            .field("shard", id())
            .field("index", getIndexName());
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        if (recoverySource != null) {
            builder.field("recovery_source", recoverySource);
        }
        if (allocationId != null) {
            builder.field("allocation_id");
            allocationId.toXContent(builder, params);
        }
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        return builder.endObject();
    }

    /**
     * Returns the expected shard size for {@link ShardRoutingState#RELOCATING} and {@link ShardRoutingState#INITIALIZING}
     * shards. If it's size is not available {@value #UNAVAILABLE_EXPECTED_SHARD_SIZE} will be returned.
     */
    public long getExpectedShardSize() {
        return expectedShardSize;
    }

    /**
     * Returns recovery source for the given shard. Replica shards always recover from the primary {@link PeerRecoverySource}.
     *
     * @return recovery source or null if shard is {@link #active()}
     */
    @Nullable
    public RecoverySource recoverySource() {
        return recoverySource;
    }
}
