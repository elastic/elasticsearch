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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * routings like id, state, version, etc.
 */
public final class ShardRouting implements Streamable, ToXContent {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;

    private String index;
    private int shardId;
    private String currentNodeId;
    private String relocatingNodeId;
    private boolean primary;
    private ShardRoutingState state;
    private long version;
    private RestoreSource restoreSource;
    private UnassignedInfo unassignedInfo;
    private AllocationId allocationId;
    private final transient List<ShardRouting> asList;
    private transient ShardId shardIdentifier;
    private boolean frozen = false;
    private long expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;

    private ShardRouting() {
        this.asList = Collections.singletonList(this);
    }

    public ShardRouting(ShardRouting copy) {
        this(copy, copy.version());
    }

    public ShardRouting(ShardRouting copy, long version) {
        this(copy.index(), copy.id(), copy.currentNodeId(), copy.relocatingNodeId(), copy.restoreSource(), copy.primary(), copy.state(), version, copy.unassignedInfo(), copy.allocationId(), true, copy.getExpectedShardSize());
    }

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    ShardRouting(String index, int shardId, String currentNodeId,
                 String relocatingNodeId, RestoreSource restoreSource, boolean primary, ShardRoutingState state, long version,
                 UnassignedInfo unassignedInfo, AllocationId allocationId, boolean internal, long expectedShardSize) {
        this.index = index;
        this.shardId = shardId;
        this.currentNodeId = currentNodeId;
        this.relocatingNodeId = relocatingNodeId;
        this.primary = primary;
        this.state = state;
        this.asList = Collections.singletonList(this);
        this.version = version;
        this.restoreSource = restoreSource;
        this.unassignedInfo = unassignedInfo;
        this.allocationId = allocationId;
        this.expectedShardSize = expectedShardSize;
        assert expectedShardSize == UNAVAILABLE_EXPECTED_SHARD_SIZE || state == ShardRoutingState.INITIALIZING || state == ShardRoutingState.RELOCATING : expectedShardSize + " state: " + state;
        assert expectedShardSize >= 0 || state != ShardRoutingState.INITIALIZING || state != ShardRoutingState.RELOCATING : expectedShardSize + " state: " + state;
        assert !(state == ShardRoutingState.UNASSIGNED && unassignedInfo == null) : "unassigned shard must be created with meta";
        if (!internal) {
            assert state == ShardRoutingState.UNASSIGNED;
            assert currentNodeId == null;
            assert relocatingNodeId == null;
            assert allocationId == null;
        }

    }

    /**
     * Creates a new unassigned shard.
     */
    public static ShardRouting newUnassigned(String index, int shardId, RestoreSource restoreSource, boolean primary, UnassignedInfo unassignedInfo) {
        return new ShardRouting(index, shardId, null, null, restoreSource, primary, ShardRoutingState.UNASSIGNED, 0, unassignedInfo, null, true, UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    /**
     * The index name.
     */
    public String index() {
        return this.index;
    }

    /**
     * The index name.
     */
    public String getIndex() {
        return index();
    }

    /**
     * The shard id.
     */
    public int id() {
        return this.shardId;
    }

    /**
     * The shard id.
     */
    public int getId() {
        return id();
    }


    /**
     * The routing version associated with the shard.
     */
    public long version() {
        return this.version;
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
     * Creates a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting buildTargetRelocatingShard() {
        assert relocating();
        return new ShardRouting(index, shardId, relocatingNodeId, currentNodeId, restoreSource, primary, ShardRoutingState.INITIALIZING, version, unassignedInfo,
            AllocationId.newTargetRelocation(allocationId), true, expectedShardSize);
    }

    /**
     * Snapshot id and repository where this shard is being restored from
     */
    public RestoreSource restoreSource() {
        return restoreSource;
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
        if (shardIdentifier != null) {
            return shardIdentifier;
        }
        shardIdentifier = new ShardId(index, shardId);
        return shardIdentifier;
    }

    public boolean allocatedPostIndexCreate() {
        if (active()) {
            return true;
        }

        // unassigned info is only cleared when a shard moves to started, so
        // for unassigned and initializing (we checked for active() before),
        // we can safely assume it is there
        if (unassignedInfo.getReason() == UnassignedInfo.Reason.INDEX_CREATED) {
            return false;
        }

        return true;
    }

    /**
     * A shard iterator with just this shard in it.
     */
    public ShardIterator shardsIt() {
        return new PlainShardIterator(shardId(), asList);
    }

    public static ShardRouting readShardRoutingEntry(StreamInput in) throws IOException {
        ShardRouting entry = new ShardRouting();
        entry.readFrom(in);
        return entry;
    }

    public static ShardRouting readShardRoutingEntry(StreamInput in, String index, int shardId) throws IOException {
        ShardRouting entry = new ShardRouting();
        entry.readFrom(in, index, shardId);
        return entry;
    }

    public void readFrom(StreamInput in, String index, int shardId) throws IOException {
        this.index = index;
        this.shardId = shardId;
        readFromThin(in);
    }

    public void readFromThin(StreamInput in) throws IOException {
        version = in.readLong();
        if (in.readBoolean()) {
            currentNodeId = in.readString();
        }

        if (in.readBoolean()) {
            relocatingNodeId = in.readString();
        }

        primary = in.readBoolean();
        state = ShardRoutingState.fromValue(in.readByte());

        restoreSource = RestoreSource.readOptionalRestoreSource(in);
        if (in.readBoolean()) {
            unassignedInfo = new UnassignedInfo(in);
        }
        if (in.readBoolean()) {
            allocationId = new AllocationId(in);
        }
        if (relocating() || initializing()) {
            expectedShardSize = in.readLong();
        } else {
            expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
        freeze();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        readFrom(in, in.readString(), in.readVInt());
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeLong(version);
        if (currentNodeId != null) {
            out.writeBoolean(true);
            out.writeString(currentNodeId);
        } else {
            out.writeBoolean(false);
        }

        if (relocatingNodeId != null) {
            out.writeBoolean(true);
            out.writeString(relocatingNodeId);
        } else {
            out.writeBoolean(false);
        }

        out.writeBoolean(primary);
        out.writeByte(state.value());

        if (restoreSource != null) {
            out.writeBoolean(true);
            restoreSource.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (unassignedInfo != null) {
            out.writeBoolean(true);
            unassignedInfo.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (allocationId != null) {
            out.writeBoolean(true);
            allocationId.writeTo(out);
        } else {
            out.writeBoolean(false);
        }
        if (relocating() || initializing()) {
            out.writeLong(expectedShardSize);
        }

    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(shardId);
        writeToThin(out);
    }

    public void updateUnassignedInfo(UnassignedInfo unassignedInfo) {
        ensureNotFrozen();
        assert this.unassignedInfo != null : "can only update unassign info if they are already set";
        this.unassignedInfo = unassignedInfo;
    }

    // package private mutators start here

    /**
     * Moves the shard to unassigned state.
     */
    void moveToUnassigned(UnassignedInfo unassignedInfo) {
        ensureNotFrozen();
        version++;
        assert state != ShardRoutingState.UNASSIGNED : this;
        state = ShardRoutingState.UNASSIGNED;
        currentNodeId = null;
        relocatingNodeId = null;
        this.unassignedInfo = unassignedInfo;
        allocationId = null;
        expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
    }

    /**
     * Initializes an unassigned shard on a node.
     */
    void initialize(String nodeId, long expectedShardSize) {
        ensureNotFrozen();
        version++;
        assert state == ShardRoutingState.UNASSIGNED : this;
        assert relocatingNodeId == null : this;
        state = ShardRoutingState.INITIALIZING;
        currentNodeId = nodeId;
        allocationId = AllocationId.newInitializing();
        this.expectedShardSize = expectedShardSize;
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard
     */
    void relocate(String relocatingNodeId, long expectedShardSize) {
        ensureNotFrozen();
        version++;
        assert state == ShardRoutingState.STARTED : "current shard has to be started in order to be relocated " + this;
        state = ShardRoutingState.RELOCATING;
        this.relocatingNodeId = relocatingNodeId;
        this.allocationId = AllocationId.newRelocation(allocationId);
        this.expectedShardSize = expectedShardSize;
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     */
    void cancelRelocation() {
        ensureNotFrozen();
        version++;
        assert state == ShardRoutingState.RELOCATING : this;
        assert assignedToNode() : this;
        assert relocatingNodeId != null : this;
        expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        state = ShardRoutingState.STARTED;
        relocatingNodeId = null;
        allocationId = AllocationId.cancelRelocation(allocationId);
    }

    /**
     * Moves the shard from started to initializing and bumps the version
     */
    void reinitializeShard() {
        ensureNotFrozen();
        assert state == ShardRoutingState.STARTED;
        version++;
        state = ShardRoutingState.INITIALIZING;
        allocationId = AllocationId.newInitializing();
        this.unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.REINITIALIZED, null);
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
     * canceled.
     */
    void moveToStarted() {
        ensureNotFrozen();
        version++;
        assert state == ShardRoutingState.INITIALIZING : "expected an initializing shard " + this;
        relocatingNodeId = null;
        restoreSource = null;
        unassignedInfo = null; // we keep the unassigned data until the shard is started
        if (allocationId.getRelocationId() != null) {
            // relocation target
            allocationId = AllocationId.finishRelocation(allocationId);
        }
        expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        state = ShardRoutingState.STARTED;
    }

    /**
     * Make the shard primary unless it's not Primary
     * //TODO: doc exception
     */
    void moveToPrimary() {
        ensureNotFrozen();
        version++;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        primary = true;
    }

    /**
     * Set the primary shard to non-primary
     */
    void moveFromPrimary() {
        ensureNotFrozen();
        version++;
        if (!primary) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        primary = false;
    }

    /** returns true if this routing has the same shardId as another */
    public boolean isSameShard(ShardRouting other) {
        return index.equals(other.index) && shardId == other.shardId;
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
     * Returns <code>true</code> if this shard is a relocation target for another shard (i.e., was created with {@link #buildTargetRelocatingShard()}
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

        assert b == false || isSameShard(other) :
            "ShardRouting is a relocation target but both routings are not of the same shard. This [" + this + "], other [" + other + "]";

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

        assert b == false || isSameShard(other) :
            "ShardRouting is a relocation source but both routings are not of the same shard. This [" + this + "], target [" + other + "]";

        assert b == false || this.primary == other.primary :
            "ShardRouting is a relocation source but primary flag is different. This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., version and unassigned info */
    public boolean equalsIgnoringMetaData(ShardRouting other) {
        if (primary != other.primary) {
            return false;
        }
        if (shardId != other.shardId) {
            return false;
        }
        if (currentNodeId != null ? !currentNodeId.equals(other.currentNodeId) : other.currentNodeId != null) {
            return false;
        }
        if (index != null ? !index.equals(other.index) : other.index != null) {
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
        if (restoreSource != null ? !restoreSource.equals(other.restoreSource) : other.restoreSource != null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        // we check on instanceof so we also handle the ImmutableShardRouting case as well
        if (o == null || !(o instanceof ShardRouting)) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        if (version != that.version) {
            return false;
        }
        if (unassignedInfo != null ? !unassignedInfo.equals(that.unassignedInfo) : that.unassignedInfo != null) {
            return false;
        }
        return equalsIgnoringMetaData(that);
    }

    private long hashVersion = version - 1;
    private int hashCode = 0;

    @Override
    public int hashCode() {
        if (hashVersion == version) {
            return hashCode;
        }
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        result = 31 * result + (currentNodeId != null ? currentNodeId.hashCode() : 0);
        result = 31 * result + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
        result = 31 * result + (primary ? 1 : 0);
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + Long.hashCode(version);
        result = 31 * result + (restoreSource != null ? restoreSource.hashCode() : 0);
        result = 31 * result + (allocationId != null ? allocationId.hashCode() : 0);
        result = 31 * result + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
        return hashCode = result;
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
        sb.append('[').append(index).append(']').append('[').append(shardId).append(']');
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        sb.append(", v[").append(version).append("]");
        if (this.restoreSource != null) {
            sb.append(", restoring[" + restoreSource + "]");
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
            .field("shard", shardId().id())
            .field("index", shardId().index().name())
            .field("version", version);
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        if (restoreSource() != null) {
            builder.field("restore_source");
            restoreSource().toXContent(builder, params);
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

    private void ensureNotFrozen() {
        if (frozen) {
            throw new IllegalStateException("ShardRouting can't be modified anymore - already frozen");
        }
    }

    void freeze() {
        frozen = true;
    }

    boolean isFrozen() {
        return frozen;
    }

    /**
     * Returns the expected shard size for {@link ShardRoutingState#RELOCATING} and {@link ShardRoutingState#INITIALIZING}
     * shards. If it's size is not available {@value #UNAVAILABLE_EXPECTED_SHARD_SIZE} will be returned.
     */
    public long getExpectedShardSize() {
        return expectedShardSize;
    }
}
