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
    public void onRecoveryCancelledBeforeQueuing(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onQueuedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onStartedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }

    @Override
    public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
        onRecoverySchedulingChange();
    }
}
