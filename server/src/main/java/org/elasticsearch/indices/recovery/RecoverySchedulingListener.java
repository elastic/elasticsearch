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
    /// than recoveries from unassigned shards. Applies only to incoming recoveries, recorded on the target, not to outgoing peer
    /// recoveries.
    enum PriorityGroup {
        UNASSIGNED,
        RELOCATION,
    }

    /// Listener that ignores every lifecycle event.
    RecoverySchedulingListener NOOP = new RecoverySchedulingListener() {};

    /// Called when an incoming recovery is directly canceled on the target by the master node, before it even reached the queue.
    default void onRecoveryCancelledBeforeQueuingOnTarget(RecoverySource.Type type) {}

    /// Called when an incoming recovery is queued on the target.
    default void onRecoveryQueuedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {}

    /// Called when an outgoing peer recovery is queued on the source.
    default void onPeerRecoveryQueuedOnSource() {}

    /// Called when a queued incoming recovery is discarded on the target without having ever run.
    default void onQueuedRecoveryDiscardedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {}

    /// Called when a queued outgoing peer recovery is discarded on the source without having ever run.
    default void onQueuedPeerRecoveryDiscardedOnSource() {}

    /// Called when a queued incoming recovery is directly canceled on the target by the master node, before it started running.
    default void onQueuedRecoveryCancelledOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {}

    /// Called when an outgoing peer recovery has been dispatched for execution on the source.
    default void onPeerRecoveryStartedOnSource() {}

    /// Called when a previously queued incoming recovery is dequeued and dispatched for execution on the target.
    default void onRecoveryDequeuedAndStartedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {}

    /// Called when a previously queued outgoing peer recovery is dequeued and dispatched for execution on the source.
    default void onPeerRecoveryDequeuedAndStartedOnSource() {}

    /// Called when started incoming recovery is directly canceled on the target by the master node.
    default void onStartedRecoveryCancelledOnTarget(RecoverySource.Type type) {}

    /// Called when a running incoming recovery finishes (success, failure or aborted) on the target.
    default void onRecoveryCompletedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {}

    /// Called when a running outgoing peer recovery finishes (success, failure or aborted) on the source.
    default void onPeerRecoveryCompletedOnSource() {}

    /// Called when this node starts holding new recoveries back due to a recovery gate; `gateName` identifies the gate. Paired with
    /// [#onRecoveriesUnblocked].
    default void onRecoveriesBlocked(String gateName) {}

    /// Called when this node stops holding recoveries back, reporting how long the block lasted (ms). Carries no gate name: the gate
    /// that started the block (reported by [#onRecoveriesBlocked]) is not necessarily the one that held it last.
    default void onRecoveriesUnblocked(long blockedTimeMillis) {}
}
