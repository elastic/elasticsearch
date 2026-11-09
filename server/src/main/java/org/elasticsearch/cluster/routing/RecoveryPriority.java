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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/// Describes the priority of a recovery. The [#ordinal()] is significant, with lower values indicating higher priorities.
public enum RecoveryPriority implements Writeable {

    /// A primary shard which is unassigned because it is newly created.
    ///
    /// This is considered high priority because recovery should be fast, so we can get this out of the way and unblock writes to the
    /// new index (including allowing bulk operations which auto-create an index to complete).
    UNASSIGNED_NEW_PRIMARY(false, true),

    /// A shard which is unassigned because of an unexpected condition, i.e. some kind of failure.
    ///
    /// Recovering this shard may be necessary to prevent the index being unavailable.
    UNASSIGNED_UNEXPECTED(false, true),

    /// A shard which is unassigned for an expected condition, i.e. because of a user operation such as opening an index.
    /// Note that this excludes [#UNASSIGNED_NEW_PRIMARY], [#UNASSIGNED_RESTORING] and [#UNASSIGNED_PERFORMANCE_SCALING].
    UNASSIGNED_EXPECTED(false, true),

    /// A shard which is assigned, and is being relocated because
    /// [org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders#canRemain] returned
    /// [org.elasticsearch.cluster.routing.allocation.decider.Decision#NO].
    RELOCATION_CAN_REMAIN_NO(true, false),

    /// A shard which is unassigned because it is being restored from a snapshot.
    ///
    /// This is lower priority than [#RELOCATION_CAN_REMAIN_NO]. Restores are expected to be a lengthy operation and
    /// the index being restored is not expected to be available until the restore completes, so delaying the restore
    /// does not make anything less available than it already is. Delaying a relocation away from a node which must be
    /// vacated (e.g. because it is shutting down) may cause a shard which is currently available to become unavailable.
    UNASSIGNED_RESTORING(false, true),

    /// A shard which is unassigned because it adds capacity to an index which is already fully available without it, e.g.:
    ///
    /// - a replica that was added to a shard which already has an active replica, or
    /// - a primary which is the target of a reshard split. Until the source hands off to the target, the index is fully
    ///   available from the source shard. Replicas of split targets are [#UNASSIGNED_EXPECTED] instead: they can only start
    ///   once the target primary has taken over its share of the writes, and the split only waits a bounded time for them
    ///   before routing searches to the target.
    ///
    /// This is lower priority than [#RELOCATION_CAN_REMAIN_NO] for the same reason as [#UNASSIGNED_RESTORING]: delaying it
    /// does not make anything less available than it already is. It is also lower priority than [#UNASSIGNED_RESTORING]
    /// because the index is already fully available without this shard, whereas a restored index only becomes available
    /// once its shards have recovered.
    UNASSIGNED_PERFORMANCE_SCALING(false, true),

    /// A shard which is assigned, and is being relocated because
    /// [org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders#canRemain] returned
    /// [org.elasticsearch.cluster.routing.allocation.decider.Decision#NOT_PREFERRED].
    RELOCATION_CAN_REMAIN_NOT_PREFERRED(true, false),

    /// A shard which is assigned, and is being relocated for rebalancing.
    RELOCATE_REBALANCING(true, false),

    /// Placeholder value for unknown priorities. This can be used in a short-lived `ShardRouting` (for example, for use in a balancing
    /// calculation) but it must _not_ be used in a `ShardRouting` which will be added to the `ClusterState`. It can also appear in a
    /// [org.elasticsearch.indices.recovery.RecoveryState] received from an older node.
    UNKNOWN(true, true);

    /// Introduced along with [#UNKNOWN] and [#UNASSIGNED_NEW_PRIMARY]
    // visible for testing
    static final TransportVersion RECOVERY_PRIORITY_UNKNOWN_AND_UNASSIGNED_NEW_PRIMARY_TRANSPORT_VERSION = TransportVersion.fromName(
        "recovery_priority_unknown_and_unassigned_new_primary"
    );

    /// Introduced along with [#UNASSIGNED_RESTORING] and [#UNASSIGNED_PERFORMANCE_SCALING]
    private static final TransportVersion RECOVERY_PRIORITY_UNASSIGNED_NON_CRITICAL_TRANSPORT_VERSION = TransportVersion.fromName(
        "recovery_priority_unassigned_non_critical"
    );

    private final boolean isValidForRelocation;
    private final boolean isValidForUnassigned;

    /**
     * Constructor.
     *
     * @param isValidForRelocation whether a shard copy that is initializing as the target of a relocation may have this priority
     * @param isValidForUnassigned whether a shard copy that is initializing from unassigned may have this priority
     */
    RecoveryPriority(boolean isValidForRelocation, boolean isValidForUnassigned) {
        this.isValidForRelocation = isValidForRelocation;
        this.isValidForUnassigned = isValidForUnassigned;
    }

    public static RecoveryPriority readFrom(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(RECOVERY_PRIORITY_UNASSIGNED_NON_CRITICAL_TRANSPORT_VERSION)) {
            return in.readEnum(RecoveryPriority.class);
        } else if (in.getTransportVersion().supports(RECOVERY_PRIORITY_UNKNOWN_AND_UNASSIGNED_NEW_PRIMARY_TRANSPORT_VERSION)) {
            // Deserialize using the ordinals from the enum prior to the introduction of the UNASSIGNED_RESTORING and
            // UNASSIGNED_PERFORMANCE_SCALING values
            int ordinal = in.readVInt();
            return switch (ordinal) {
                case 0 -> UNASSIGNED_NEW_PRIMARY;
                case 1 -> UNASSIGNED_UNEXPECTED;
                case 2 -> UNASSIGNED_EXPECTED;
                case 3 -> RELOCATION_CAN_REMAIN_NO;
                case 4 -> RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                case 5 -> RELOCATE_REBALANCING;
                case 6 -> UNKNOWN;
                default -> throw new IllegalArgumentException("Unexpected value for legacy RecoveryPriority ordinal: " + ordinal);
            };
        } else {
            // Deserialize using the ordinals from the enum prior to the introduction of the UNKNOWN and UNASSIGNED_NEW_PRIMARY values:
            int legacyOrdinal = in.readVInt();
            return switch (legacyOrdinal) {
                case 0 -> UNASSIGNED_UNEXPECTED;
                case 1 -> UNASSIGNED_EXPECTED;
                case 2 -> RELOCATION_CAN_REMAIN_NO;
                case 3 -> RELOCATION_CAN_REMAIN_NOT_PREFERRED;
                case 4 -> RELOCATE_REBALANCING;
                default -> throw new IllegalArgumentException("Unexpected value for legacy RecoveryPriority ordinal: " + legacyOrdinal);
            };
        }
    }

    // visible for testing
    boolean isValidForRelocation() {
        return isValidForRelocation;
    }

    // visible for testing
    public boolean isValidForUnassigned() {
        return isValidForUnassigned;
    }

    /// Returns whether a recovery with this priority is critical, i.e. one for a shard copy that the client expects
    /// to be available but currently isn't. Relocations (whose source stays active until they complete), recoveries
    /// of unassigned shards added for performance reasons, and snapshot restores (whose index is expected to be
    /// unavailable until they complete, which may take a long time) are non-critical. Note that non-critical recoveries
    /// are subject to additional throttling (see [org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings]).
    ///
    /// All the critical priorities must rank above all the non-critical ones, which
    /// [org.elasticsearch.indices.recovery.ThrottlingRecoveryService] relies on when dispatching queued recoveries.
    public boolean isCritical() {
        return switch (this) {
            case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED, UNASSIGNED_EXPECTED -> true;
            case RELOCATION_CAN_REMAIN_NO, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING, RELOCATION_CAN_REMAIN_NOT_PREFERRED,
                RELOCATE_REBALANCING -> false;
            // UNKNOWN should never be seen in cluster state. We treat it here as the lowest priority.
            case UNKNOWN -> false;
        };
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(RECOVERY_PRIORITY_UNASSIGNED_NON_CRITICAL_TRANSPORT_VERSION)) {
            out.writeEnum(this);
        } else if (out.getTransportVersion().supports(RECOVERY_PRIORITY_UNKNOWN_AND_UNASSIGNED_NEW_PRIMARY_TRANSPORT_VERSION)) {
            // Serialize using the ordinals from the enum prior to the introduction of the UNASSIGNED_RESTORING and
            // UNASSIGNED_PERFORMANCE_SCALING values, or the nearest fallbacks
            int ordinal = switch (this) {
                case UNASSIGNED_NEW_PRIMARY -> 0;
                case UNASSIGNED_UNEXPECTED -> 1;
                case UNASSIGNED_EXPECTED, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING -> 2;
                case RELOCATION_CAN_REMAIN_NO -> 3;
                case RELOCATION_CAN_REMAIN_NOT_PREFERRED -> 4;
                case RELOCATE_REBALANCING -> 5;
                case UNKNOWN -> 6;
            };
            out.writeVInt(ordinal);
        } else {
            // Serialize using the ordinals from the enum prior to the introduction of the UNKNOWN and UNASSIGNED_NEW_PRIMARY values,
            // or the nearest fallbacks:
            int legacyOrdinal = switch (this) {
                case UNASSIGNED_NEW_PRIMARY, UNASSIGNED_UNEXPECTED -> 0;
                case UNASSIGNED_EXPECTED, UNASSIGNED_RESTORING, UNASSIGNED_PERFORMANCE_SCALING -> 1;
                case RELOCATION_CAN_REMAIN_NO -> 2;
                case RELOCATION_CAN_REMAIN_NOT_PREFERRED -> 3;
                case RELOCATE_REBALANCING, UNKNOWN -> 4;
            };
            out.writeVInt(legacyOrdinal);
        }
    }
}
