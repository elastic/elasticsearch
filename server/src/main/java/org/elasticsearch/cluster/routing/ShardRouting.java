/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource.ExistingStoreRecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.PeerRecoverySource;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * {@link ShardRouting} immutably encapsulates information about shard
 * indexRoutings like id, state, version, etc.
 *
 * Information about a particular shard instance.
 */
public final class ShardRouting implements Writeable, ToXContentObject {

    /**
     * Used if shard size is not available
     */
    public static final long UNAVAILABLE_EXPECTED_SHARD_SIZE = -1;
    private static final TransportVersion EXPECTED_SHARD_SIZE_FOR_STARTED_VERSION = TransportVersions.V_8_5_0;
    private static final TransportVersion RELOCATION_FAILURE_INFO_VERSION = TransportVersions.V_8_6_0;

    private final ShardId shardId;
    private final boolean primary;
    private final Role role;
    private final RelocationFailureInfo relocationFailureInfo;
    private final ShardState state;

    ShardRouting(
        ShardId shardId,
        String currentNodeId,
        String relocatingNodeId,
        boolean primary,
        ShardRoutingState state,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo,
        RelocationFailureInfo relocationFailureInfo,
        AllocationId allocationId,
        long expectedShardSize,
        Role role
    ) {
        this.shardId = shardId;
        this.primary = primary;
        this.role = role;
        this.relocationFailureInfo = relocationFailureInfo;
        this.state = switch (state) {
            case UNASSIGNED -> new Unassigned(unassignedInfo, recoverySource);
            case INITIALIZING -> new Initializing(
                currentNodeId,
                relocatingNodeId,
                unassignedInfo,
                recoverySource,
                allocationId,
                expectedShardSize
            );
            case STARTED -> new Started(currentNodeId, allocationId, expectedShardSize);
            case RELOCATING -> newRelocatingState(
                shardId,
                primary,
                role,
                currentNodeId,
                relocatingNodeId,
                unassignedInfo,
                allocationId,
                expectedShardSize
            );
        };
    }

    sealed interface ShardState permits Initializing, Relocating, Started, Unassigned {
        ShardRoutingState name();
    }

    sealed interface AssignedState permits Initializing, Relocating, Started {
        String currentNodeId();

        AllocationId allocationId();

        long expectedShardSize();
    }

    sealed interface RecoveryState permits Unassigned, Initializing {
        RecoverySource recoverySource();

        UnassignedInfo unassignedInfo();
    }

    record Unassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource) implements ShardState, RecoveryState {
        @Override
        public ShardRoutingState name() {
            return ShardRoutingState.UNASSIGNED;
        }
    }

    record Initializing(
        String currentNodeId,
        @Nullable String relocatingNodeId,
        @Nullable UnassignedInfo unassignedInfo,
        RecoverySource recoverySource,
        AllocationId allocationId,
        long expectedShardSize
    ) implements ShardState, AssignedState, RecoveryState {

        @Override
        public ShardRoutingState name() {
            return ShardRoutingState.INITIALIZING;
        }
    }

    record Started(String currentNodeId, AllocationId allocationId, long expectedShardSize) implements ShardState, AssignedState {
        @Override
        public ShardRoutingState name() {
            return ShardRoutingState.STARTED;
        }
    }

    record Relocating(
        String currentNodeId,
        String relocatingNodeId,
        AllocationId allocationId,
        ShardRouting targetRelocatingShard,
        long expectedShardSize
    ) implements ShardState, AssignedState {
        @Override
        public ShardRoutingState name() {
            return ShardRoutingState.RELOCATING;
        }
    }

    /**
     * A constructor to internally create shard routing instances, note, the internal flag should only be set to true
     * by either this class or tests. Visible for testing.
     */
    ShardRouting(ShardId shardId, boolean primary, Role role, RelocationFailureInfo relocationFailureInfo, ShardState shardState) {
        this.shardId = shardId;
        this.primary = primary;
        this.role = role;
        this.relocationFailureInfo = relocationFailureInfo;
        this.state = shardState;
        assert assertConsistent();
    }

    private boolean assertConsistent() {
        assert relocationFailureInfo != null : "relocation failure info must be always set";
        assert role != null : "role must be always set";
        assert primary == false || role.isPromotableToPrimary() : "shard with unpromotable role was promoted to primary: " + this;
        switch (state) {
            case Unassigned unassigned -> {
                assert primary ^ unassigned.recoverySource() == PeerRecoverySource.INSTANCE
                    : "replica shards always recover from primary" + this;
            }
            case Initializing initializing -> {
                assert initializing.currentNodeId != null : "shard must be assigned to a node " + this;
                ;
                assert primary || initializing.recoverySource() == PeerRecoverySource.INSTANCE
                    : "replica shards always recover from primary" + this;
            }
            case Started started -> {
                assert started.currentNodeId() != null : "shard must be assigned to a node " + this;
            }
            case Relocating relocating -> {
                assert relocating.currentNodeId() != null : "shard must be assigned to a node " + this;
                assert relocating.relocatingNodeId() != null : "shard must be relocating to a node " + this;
            }
        }
        return true;
    }

    static Relocating newRelocatingState(
        ShardId shardId,
        boolean primary,
        Role role,
        String currentNodeId,
        String relocatingNodeId,
        UnassignedInfo unassignedInfo,
        AllocationId allocationId,
        long expectedShardSize
    ) {
        return new Relocating(
            currentNodeId,
            relocatingNodeId,
            allocationId,
            new ShardRouting(
                shardId,
                primary,
                role,
                RelocationFailureInfo.NO_FAILURES,
                new Initializing(
                    relocatingNodeId,
                    currentNodeId,
                    unassignedInfo,
                    PeerRecoverySource.INSTANCE,
                    AllocationId.newTargetRelocation(allocationId),
                    expectedShardSize
                )
            ),
            expectedShardSize
        );
    }

    /**
     * Creates a new unassigned shard.
     */
    public static ShardRouting newUnassigned(
        ShardId shardId,
        boolean primary,
        RecoverySource recoverySource,
        UnassignedInfo unassignedInfo,
        Role role
    ) {
        return new ShardRouting(shardId, primary, role, RelocationFailureInfo.NO_FAILURES, new Unassigned(unassignedInfo, recoverySource));
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
        return state instanceof Unassigned;
    }

    /**
     * The shard is initializing (usually recovering either from peer shard
     * or from gateway).
     */
    public boolean initializing() {
        return state instanceof Initializing;
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
        return state instanceof Started;
    }

    /**
     * Returns <code>true</code> iff this shard is currently relocating to
     * another node. Otherwise <code>false</code>
     *
     * @see ShardRoutingState#RELOCATING
     */
    public boolean relocating() {
        return state instanceof Relocating;
    }

    /**
     * Returns <code>true</code> iff this shard is assigned to a node ie. not
     * {@link ShardRoutingState#UNASSIGNED unassigned}. Otherwise <code>false</code>
     */
    public boolean assignedToNode() {
        return unassigned() == false;
    }

    /**
     * The current node id the shard is allocated on.
     */
    public String currentNodeId() {
        if (state instanceof AssignedState s) {
            return s.currentNodeId();
        } else {
            return null;
        }
    }

    /**
     * The relocating node id the shard is either relocating to or relocating from.
     */
    public String relocatingNodeId() {
        return switch (state) {
            case Relocating r -> r.relocatingNodeId;
            case Initializing i -> i.relocatingNodeId;
            default -> null;
        };
    }

    /**
     * Returns a shard routing representing the target shard.
     * The target shard routing will be the INITIALIZING state and have relocatingNodeId set to the
     * source node.
     */
    public ShardRouting getTargetRelocatingShard() {
        assert relocating();
        return ((Relocating) state).targetRelocatingShard;
    }

    /**
     * Additional metadata on why the shard is/was unassigned. The metadata is kept around
     * until the shard moves to STARTED.
     */
    @Nullable
    public UnassignedInfo unassignedInfo() {
        if (state instanceof RecoveryState s) {
            return s.unassignedInfo();
        } else {
            return null;
        }
    }

    @Nullable
    public RelocationFailureInfo relocationFailureInfo() {
        return relocationFailureInfo;
    }

    /**
     * An id that uniquely identifies an allocation.
     */
    @Nullable
    public AllocationId allocationId() {
        if (state instanceof AssignedState s) {
            return s.allocationId();
        }
        return null;
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
        return this.state.name();
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
        return new ShardIterator(shardId, List.of(this));
    }

    public ShardRouting(ShardId shardId, StreamInput in) throws IOException {
        this.shardId = shardId;
        var currentNodeId = DiscoveryNode.deduplicateNodeIdentifier(in.readOptionalString());
        var relocatingNodeId = DiscoveryNode.deduplicateNodeIdentifier(in.readOptionalString());
        primary = in.readBoolean();
        var stateName = ShardRoutingState.fromValue(in.readByte());
        RecoverySource recoverySource = null;
        if (stateName == ShardRoutingState.UNASSIGNED || stateName == ShardRoutingState.INITIALIZING) {
            recoverySource = RecoverySource.readFrom(in);
        }
        var unassignedInfo = in.readOptionalWriteable(UnassignedInfo::fromStreamInput);
        if (in.getTransportVersion().onOrAfter(RELOCATION_FAILURE_INFO_VERSION)) {
            relocationFailureInfo = RelocationFailureInfo.readFrom(in);
        } else {
            relocationFailureInfo = RelocationFailureInfo.NO_FAILURES;
        }
        var allocationId = in.readOptionalWriteable(AllocationId::new);
        var expectedShardSize = UNAVAILABLE_EXPECTED_SHARD_SIZE;
        if (stateName == ShardRoutingState.RELOCATING
            || stateName == ShardRoutingState.INITIALIZING
            || (stateName == ShardRoutingState.STARTED && in.getTransportVersion().onOrAfter(EXPECTED_SHARD_SIZE_FOR_STARTED_VERSION))) {
            expectedShardSize = in.readLong();
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            role = Role.readFrom(in);
        } else {
            role = Role.DEFAULT;
        }
        switch (stateName) {
            case UNASSIGNED -> this.state = new Unassigned(unassignedInfo, recoverySource);
            case INITIALIZING -> this.state = new Initializing(
                currentNodeId,
                relocatingNodeId,
                unassignedInfo,
                recoverySource,
                allocationId,
                expectedShardSize
            );
            case STARTED -> this.state = new Started(currentNodeId, allocationId, expectedShardSize);
            case RELOCATING -> this.state = newRelocatingState(
                shardId,
                primary,
                role,
                currentNodeId,
                relocatingNodeId,
                unassignedInfo,
                allocationId,
                expectedShardSize
            );
            default -> throw new IllegalStateException();
        }
    }

    public ShardRouting(StreamInput in) throws IOException {
        this(new ShardId(in), in);
    }

    /**
     * Writes shard information to {@link StreamOutput} without writing index name and shard id
     *
     * @param out {@link StreamOutput} to write shard information to
     * @throws IOException if something happens during write
     */
    public void writeToThin(StreamOutput out) throws IOException {
        out.writeOptionalString(currentNodeId());
        out.writeOptionalString(relocatingNodeId());
        out.writeBoolean(primary);
        out.writeByte(state.name().value());
        if (state instanceof RecoveryState rs) {
            rs.recoverySource().writeTo(out);
        }
        out.writeOptionalWriteable(unassignedInfo());
        if (out.getTransportVersion().onOrAfter(RELOCATION_FAILURE_INFO_VERSION)) {
            relocationFailureInfo.writeTo(out);
        }
        out.writeOptionalWriteable(allocationId());
        if (relocating() || initializing() || (started() && out.getTransportVersion().onOrAfter(EXPECTED_SHARD_SIZE_FOR_STARTED_VERSION))) {
            out.writeLong(getExpectedShardSize());
        }

        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_7_0)) {
            role.writeTo(out);
        } else if (role != Role.DEFAULT) {
            throw new IllegalStateException(
                Strings.format("cannot send role [%s] to node with version [%s]", role, out.getTransportVersion().toReleaseVersion())
            );
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shardId.writeTo(out);
        writeToThin(out);
    }

    public ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource) {
        assert unassigned();
        final var stateUnassigned = (Unassigned) state;
        assert stateUnassigned.unassignedInfo != null : "can only update unassigned info if it is already set";
        assert stateUnassigned.unassignedInfo.delayed() || (unassignedInfo.delayed() == false)
            : "cannot transition from non-delayed to delayed";
        return new ShardRouting(shardId, primary, role, relocationFailureInfo, new Unassigned(unassignedInfo, recoverySource));
    }

    public ShardRouting updateRelocationFailure(RelocationFailureInfo relocationFailureInfo) {
        assert this.relocationFailureInfo != null : "can only update relocation failure info info if it is already set";
        return new ShardRouting(shardId, primary, role, relocationFailureInfo, state);
    }

    /**
     * Moves the shard to unassigned state.
     */
    public ShardRouting moveToUnassigned(UnassignedInfo unassignedInfo) {
        assert unassigned() == false : this;
        final RecoverySource recoverySource;
        if (active()) {
            if (primary()) {
                recoverySource = ExistingStoreRecoverySource.INSTANCE;
            } else {
                recoverySource = PeerRecoverySource.INSTANCE;
            }
        } else {
            recoverySource = recoverySource();
        }
        return new ShardRouting(shardId, primary, role, RelocationFailureInfo.NO_FAILURES, new Unassigned(unassignedInfo, recoverySource));
    }

    /**
     * Initializes an unassigned shard on a node.
     *
     * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
     */
    public ShardRouting initialize(String nodeId, @Nullable String existingAllocationId, long expectedShardSize) {
        assert unassigned() : this;
        final var stateUnassigned = (Unassigned) state;
        final AllocationId allocationId;
        if (existingAllocationId == null) {
            allocationId = AllocationId.newInitializing();
        } else {
            allocationId = AllocationId.newInitializing(existingAllocationId);
        }
        return new ShardRouting(
            shardId,
            primary,
            role,
            RelocationFailureInfo.NO_FAILURES,
            new Initializing(nodeId, null, stateUnassigned.unassignedInfo, stateUnassigned.recoverySource, allocationId, expectedShardSize)
        );
    }

    /**
     * Relocate the shard to another node.
     *
     * @param relocatingNodeId id of the node to relocate the shard
     */
    public ShardRouting relocate(String relocatingNodeId, long expectedShardSize) {
        assert started() : "current shard has to be started in order to be relocated " + this;
        final var stateStarted = (Started) state;
        return new ShardRouting(
            shardId,
            primary,
            role,
            relocationFailureInfo,
            newRelocatingState(
                shardId,
                primary,
                role,
                stateStarted.currentNodeId,
                relocatingNodeId,
                null,
                AllocationId.newRelocation(stateStarted.allocationId),
                expectedShardSize
            )
        );
    }

    /**
     * Cancel relocation of a shard. The shards state must be set
     * to <code>RELOCATING</code>.
     */
    public ShardRouting cancelRelocation() {
        assert relocating() : this;
        assert assignedToNode() : this;
        final var stateRelocating = (Relocating) state;
        assert stateRelocating.relocatingNodeId != null : this;
        return new ShardRouting(
            shardId,
            primary,
            role,
            relocationFailureInfo.incFailedRelocations(),
            new Started(
                stateRelocating.currentNodeId,
                AllocationId.cancelRelocation(stateRelocating.allocationId),
                stateRelocating.expectedShardSize
            )
        );
    }

    /**
     * Removes relocation source of a non-primary shard. The shard state must be <code>INITIALIZING</code>.
     * This allows the non-primary shard to continue recovery from the primary even though its non-primary
     * relocation source has failed.
     */
    public ShardRouting removeRelocationSource() {
        assert primary == false : this;
        assert initializing() : this;
        assert assignedToNode() : this;
        final var stateInit = (Initializing) state;
        assert stateInit.relocatingNodeId != null : this;
        return new ShardRouting(
            shardId,
            primary,
            role,
            relocationFailureInfo,
            new Initializing(
                stateInit.currentNodeId,
                null,
                stateInit.unassignedInfo,
                stateInit.recoverySource,
                AllocationId.finishRelocation(stateInit.allocationId),
                stateInit.expectedShardSize
            )
        );
    }

    /**
     * Reinitializes a replica shard, giving it a fresh allocation id
     */
    public ShardRouting reinitializeReplicaShard() {
        assert initializing() : this;
        assert primary == false : this;
        final var stateInit = (Initializing) state;
        assert stateInit.relocatingNodeId == null : this;
        return new ShardRouting(
            shardId,
            primary,
            role,
            relocationFailureInfo,
            new Initializing(
                stateInit.currentNodeId,
                null,
                stateInit.unassignedInfo,
                stateInit.recoverySource,
                AllocationId.newInitializing(),
                stateInit.expectedShardSize
            )
        );
    }

    /**
     * Set the shards state to <code>STARTED</code>. The shards state must be
     * <code>INITIALIZING</code> or <code>RELOCATING</code>. Any relocation will be
     * canceled.
     */
    public ShardRouting moveToStarted(long expectedShardSize) {
        assert initializing() : "expected an initializing shard " + this;
        final var stateInit = (Initializing) state;
        var allocationId = stateInit.allocationId;
        if (allocationId.getRelocationId() != null) {
            // relocation target
            allocationId = AllocationId.finishRelocation(allocationId);
        }
        return new ShardRouting(
            shardId,
            primary,
            role,
            RelocationFailureInfo.NO_FAILURES,
            new Started(stateInit.currentNodeId, allocationId, expectedShardSize)
        );
    }

    /**
     * Make the active shard primary unless it's not primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a primary
     */
    public ShardRouting moveActiveReplicaToPrimary() {
        assert active() : "expected an active shard " + this;
        if (primary) {
            throw new IllegalShardRoutingStateException(this, "Already primary, can't move to primary");
        }
        return new ShardRouting(shardId, true, role, relocationFailureInfo, state);
    }

    /**
     * Set the unassigned primary shard to non-primary
     *
     * @throws IllegalShardRoutingStateException if shard is already a replica
     */
    public ShardRouting moveUnassignedFromPrimary() {
        assert unassigned() : "expected an unassigned shard " + this;
        if (primary == false) {
            throw new IllegalShardRoutingStateException(this, "Not primary, can't move to replica");
        }
        final var stateUnassigned = (Unassigned) state;
        return new ShardRouting(
            shardId,
            false,
            role,
            relocationFailureInfo,
            new Unassigned(stateUnassigned.unassignedInfo, PeerRecoverySource.INSTANCE)
        );
    }

    /**
     * returns true if this routing has the same allocation ID as another.
     * <p>
     * Note: if both shard routing has a null as their {@link #allocationId()}, this method returns false as the routing describe
     * no allocation at all..
     **/
    public boolean isSameAllocation(ShardRouting other) {
        if (state instanceof AssignedState thisState && other.state instanceof AssignedState otherState) {
            boolean b = thisState.allocationId() != null
                && otherState.allocationId() != null
                && thisState.allocationId().getId().equals(otherState.allocationId().getId());
            assert b == false || thisState.currentNodeId().equals(otherState.currentNodeId())
                : "ShardRoutings have the same allocation id but not the same node. This [" + this + "], other [" + other + "]";
            return b;
        } else {
            return false;
        }
    }

    /**
     * Returns <code>true</code> if this shard is a relocation target for another shard
     */
    public boolean isRelocationTarget() {
        if (state instanceof Initializing i) {
            return i.relocatingNodeId != null;
        } else {
            return false;
        }
    }

    /** returns true if the routing is the relocation target of the given routing */
    public boolean isRelocationTargetOf(ShardRouting other) {

        boolean b = false;
        if (this.state instanceof Initializing thisState && other.state instanceof Relocating otherState) {
            b = thisState.allocationId != null
                && otherState.allocationId != null
                && thisState.allocationId.getId().equals(otherState.allocationId.getRelocationId());
        }

        assert b == false || other.relocating()
            : "ShardRouting is a relocation target but the source shard state isn't relocating. This [" + this + "], other [" + other + "]";

        assert b == false || other.allocationId().getId().equals(this.allocationId().getRelocationId())
            : "ShardRouting is a relocation target but the source id isn't equal to source's allocationId.getRelocationId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId())
            : "ShardRouting is a relocation target but source current node id isn't equal to target relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId())
            : "ShardRouting is a relocation target but current node id isn't equal to source relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.shardId.equals(other.shardId)
            : "ShardRouting is a relocation target but both indexRoutings are not of the same shard id."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.primary == other.primary
            : "ShardRouting is a relocation target but primary flag is different." + " This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the routing is the relocation source for the given routing */
    public boolean isRelocationSourceOf(ShardRouting other) {
        boolean b = false;
        if (this.state instanceof Relocating thisState && other.state instanceof Initializing otherState) {
            b = thisState.allocationId != null
                && otherState.allocationId != null
                && otherState.allocationId.getId().equals(thisState.allocationId.getRelocationId());
        }

        assert b == false || relocating()
            : "ShardRouting is a relocation source but shard state isn't relocating. This [" + this + "], other [" + other + "]";

        assert b == false || this.allocationId().getId().equals(other.allocationId().getRelocationId())
            : "ShardRouting is a relocation source but the allocation id isn't equal to other.allocationId.getRelocationId."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.currentNodeId().equals(other.relocatingNodeId())
            : "ShardRouting is a relocation source but current node isn't equal to other's relocating node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || other.currentNodeId().equals(this.relocatingNodeId())
            : "ShardRouting is a relocation source but relocating node isn't equal to other's current node."
                + " This ["
                + this
                + "], other ["
                + other
                + "]";

        assert b == false || this.shardId.equals(other.shardId)
            : "ShardRouting is a relocation source but both indexRoutings are not of the same shard."
                + " This ["
                + this
                + "], target ["
                + other
                + "]";

        assert b == false || this.primary == other.primary
            : "ShardRouting is a relocation source but primary flag is different. This [" + this + "], target [" + other + "]";

        return b;
    }

    /** returns true if the current routing is identical to the other routing in all but meta fields, i.e., unassigned info */
    public boolean equalsIgnoringMetadata(ShardRouting other) {
        return primary == other.primary
            && shardId.equals(other.shardId)
            && role == other.role
            && Objects.equals(currentNodeId(), other.currentNodeId())
            && Objects.equals(relocatingNodeId(), other.relocatingNodeId())
            && Objects.equals(allocationId(), other.allocationId())
            && Objects.equals(recoverySource(), other.recoverySource());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ShardRouting that = (ShardRouting) o;
        return equalsIgnoringMetadata(that)
            && Objects.equals(unassignedInfo(), that.unassignedInfo())
            && Objects.equals(relocationFailureInfo, that.relocationFailureInfo);
    }

    /**
     * Cache hash code in the same way as {@link String#hashCode()}) using racy single-check idiom
     * as it is mainly used in single-threaded code ({@link BalancedShardsAllocator}).
     */
    private int hashCode; // default to 0

    @Override
    public int hashCode() {
        int h = hashCode;
        var currentNodeId = currentNodeId();
        var relocatingNodeId = relocatingNodeId();
        var recoverySource = recoverySource();
        var allocationId = allocationId();
        var unassignedInfo = unassignedInfo();
        if (h == 0) {
            h = shardId.hashCode();
            h = 31 * h + (currentNodeId != null ? currentNodeId.hashCode() : 0);
            h = 31 * h + (relocatingNodeId != null ? relocatingNodeId.hashCode() : 0);
            h = 31 * h + (primary ? 1 : 0);
            h = 31 * h + (state != null ? state.hashCode() : 0);
            h = 31 * h + (recoverySource != null ? recoverySource.hashCode() : 0);
            h = 31 * h + (allocationId != null ? allocationId.hashCode() : 0);
            h = 31 * h + (unassignedInfo != null ? unassignedInfo.hashCode() : 0);
            h = 31 * h + (relocationFailureInfo != null ? relocationFailureInfo.hashCode() : 0);
            h = 31 * h + role.hashCode();
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
        var currentNodeId = currentNodeId();
        var relocatingNodeId = relocatingNodeId();
        var recoverySource = recoverySource();
        var allocationId = allocationId();
        var unassignedInfo = unassignedInfo();
        var expectedShardSize = getExpectedShardSize();
        sb.append(", node[").append(currentNodeId).append("], ");
        if (relocatingNodeId != null) {
            sb.append("relocating [").append(relocatingNodeId).append("], ");
        }
        if (role != Role.DEFAULT) {
            sb.append("[").append(role).append("], ");
        }
        if (primary) {
            sb.append("[P]");
        } else {
            sb.append("[R]");
        }
        if (recoverySource != null) {
            sb.append(", recovery_source[").append(recoverySource).append("]");
        }
        sb.append(", s[").append(state.name()).append("]");
        if (allocationId != null) {
            sb.append(", a").append(allocationId);
        }
        if (unassignedInfo != null) {
            sb.append(", ").append(unassignedInfo);
        }
        sb.append(", ").append(relocationFailureInfo);
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
        var expectedShardSize = getExpectedShardSize();
        if (expectedShardSize != UNAVAILABLE_EXPECTED_SHARD_SIZE && started() == false) {
            builder.field("expected_shard_size_in_bytes", expectedShardSize);
        }
        var recoverySource = recoverySource();
        if (recoverySource != null) {
            builder.field("recovery_source", recoverySource);
        }
        var allocationId = allocationId();
        if (allocationId != null) {
            builder.field("allocation_id");
            allocationId.toXContent(builder, params);
        }
        var unassignedInfo = unassignedInfo();
        if (unassignedInfo != null) {
            unassignedInfo.toXContent(builder, params);
        }
        relocationFailureInfo.toXContent(builder, params);
        role.toXContent(builder, params);
        return builder.endObject();
    }

    /**
     * Returns the expected shard size for {@link ShardRoutingState#RELOCATING} and {@link ShardRoutingState#INITIALIZING}
     * shards. If it's size is not available {@value #UNAVAILABLE_EXPECTED_SHARD_SIZE} will be returned.
     */
    public long getExpectedShardSize() {
        if (state instanceof AssignedState s) {
            return s.expectedShardSize();
        } else {
            return UNAVAILABLE_EXPECTED_SHARD_SIZE;
        }
    }

    /**
     * Returns recovery source for the given shard. Replica shards always recover from the primary {@link PeerRecoverySource}.
     *
     * @return recovery source or null if shard is {@link #active()}
     */
    @Nullable
    public RecoverySource recoverySource() {
        if (state instanceof RecoveryState rs) {
            return rs.recoverySource();
        } else {
            return null;
        }
    }

    public Role role() {
        return role;
    }

    public boolean isPromotableToPrimary() {
        return role.isPromotableToPrimary();
    }

    /**
     * Determine if role searchable. Consumers should prefer {@link IndexRoutingTable#readyForSearch()} to determine if an index
     * is ready to be searched.
     */
    public boolean isSearchable() {
        return role.isSearchable();
    }

    public enum Role implements Writeable, ToXContentFragment {
        DEFAULT((byte) 0, true, true),
        INDEX_ONLY((byte) 1, true, false),
        SEARCH_ONLY((byte) 2, false, true);

        private final byte code;
        private final boolean promotable;
        private final boolean searchable;

        Role(byte code, boolean promotable, boolean searchable) {
            this.code = code;
            this.promotable = promotable;
            this.searchable = searchable;
        }

        /**
         * @return whether a shard copy with this role may be promoted from replica to primary. If {@code index.number_of_replicas} is
         * reduced, unpromotable replicas are removed first.
         */
        public boolean isPromotableToPrimary() {
            return promotable;
        }

        /**
         * @return whether a shard copy with this role may be the target of a search.
         */
        public boolean isSearchable() {
            return searchable;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return this == DEFAULT ? builder : builder.field("role", toString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeByte(code);
        }

        public static Role readFrom(StreamInput in) throws IOException {
            return switch (in.readByte()) {
                case 0 -> DEFAULT;
                case 1 -> INDEX_ONLY;
                case 2 -> SEARCH_ONLY;
                default -> throw new IllegalStateException("unknown role");
            };
        }
    }
}
