/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.cluster.routing.RecoverySource;

/// Listener for recovery scheduling changes. Invoked when a recovery starts, ends, or is queued/dequeued.
///
/// Implementations must be thread-safe, not block, and not throw exceptions.
///
/// Default methods cover every lifecycle transition and are no-ops by default, so implementers only override the events
/// they care about.
public interface RecoverySchedulingListener {

    /// Enumerates the priority groups for a recovery. These groups can affect throttling, e.g. we can throttle relocations more tightly
    /// than recoveries from unassigned shards. Applies only to the target (in the [RecoveryRole] sense), not the source.
    enum PriorityGroup {
        /** Recovering an unassigned shard */
        UNASSIGNED,
        /** Recovering a shard to relocate it */
        RELOCATION,
    }

    /// Listener that ignores every lifecycle event.
    RecoverySchedulingListener NOOP = new RecoverySchedulingListener() {};

    /// Called when a recovery is directly cancelled by the master node, before it even reached the queue.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    default void onRecoveryCancelledBeforeQueuing(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a recovery is queued on this data node.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when a queued recovery is discarded without having ever run.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when a queued recovery is directly cancelled by the master node, before it started running.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onQueuedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when a recovery has been dispatched for execution on this data node.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when a previously queued recovery is dequeued and dispatched for execution on this data node.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when started recovery is directly cancelled by the master node.
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    default void onStartedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a running recovery finishes (success, failure or aborted).
    ///
    /// @param type The type of source being recovered from.
    /// @param role Whether this is the `SOURCE` or `TARGET` of the recovery. `SOURCE` is allowed only when `type` is `PEER`. `TARGET` is
    /// allowed for all `type` values.
    /// @param priorityGroup When `role` is `TARGET`, indicates whether this is an `UNASSIGNED` or `RELOCATION` recovery. When `role` is
    /// `SOURCE`, must be null.
    default void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role, PriorityGroup priorityGroup) {}

    /// Called when this node starts holding new recoveries back due to a recovery gate; `gateName` identifies the gate. Paired with
    /// [#onRecoveriesUnblocked].
    default void onRecoveriesBlocked(String gateName) {}

    /// Called when this node stops holding recoveries back, reporting how long the block lasted (ms). Carries no gate name: the gate
    /// that started the block (reported by [#onRecoveriesBlocked]) is not necessarily the one that held it last.
    default void onRecoveriesUnblocked(long blockedTimeMillis) {}
}
