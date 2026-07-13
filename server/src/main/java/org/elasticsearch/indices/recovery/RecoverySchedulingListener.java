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

    /// Listener that ignores every lifecycle event.
    RecoverySchedulingListener NOOP = new RecoverySchedulingListener() {};

    /// Called when a recovery is directly cancelled by the master node, before it even reached the queue.
    default void onRecoveryCancelledBeforeQueuing(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a recovery is queued on this data node.
    default void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a queued recovery is discarded without having ever run.
    default void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a queued recovery is directly cancelled by the master node, before it started running.
    default void onQueuedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a recovery has been dispatched for execution on this data node.
    default void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a previously queued recovery is dequeued and dispatched for execution on this data node.
    default void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when started recovery is directly cancelled by the master node.
    default void onStartedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {}

    /// Called when a running recovery finishes (success, failure or aborted).
    default void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {}
}
