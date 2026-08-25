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

/// A [RecoverySchedulingListener] that calls [#onRecoverySchedulingChange] on every scheduling event.
/// Subclasses implement [#onRecoverySchedulingChange] to react to any recovery scheduling transition.
public abstract class TestRecoverySchedulingListener implements RecoverySchedulingListener {

    /// Called whenever any recovery scheduling event fires.
    public abstract void onRecoverySchedulingChange();

    @Override
    public void onRecoveryCancelledBeforeQueuingOnTarget(RecoverySource.Type type) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryQueuedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onPeerRecoveryQueuedOnSource() {
        onRecoverySchedulingChange();
    }

    @Override
    public void onQueuedRecoveryDiscardedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onQueuedPeerRecoveryDiscardedOnSource() {
        onRecoverySchedulingChange();
    }

    @Override
    public void onQueuedRecoveryCancelledOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onPeerRecoveryStartedOnSource() {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryDequeuedAndStartedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onPeerRecoveryDequeuedAndStartedOnSource() {
        onRecoverySchedulingChange();
    }

    @Override
    public void onStartedRecoveryCancelledOnTarget(RecoverySource.Type type) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryCompletedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onPeerRecoveryCompletedOnSource() {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveriesBlocked(String gateName) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveriesUnblocked(long blockedTimeMillis) {
        onRecoverySchedulingChange();
    }
}
