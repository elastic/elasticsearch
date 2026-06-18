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

/// A [RecoverySchedulingListener] that calls a provided [Runnable] on every scheduling event.
public class TestRecoverySchedulingListener implements RecoverySchedulingListener {
    private final Runnable onChange;

    public TestRecoverySchedulingListener(Runnable onChange) {
        this.onChange = onChange;
    }

    @Override
    public void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role) {
        onChange.run();
    }

    @Override
    public void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role) {
        onChange.run();
    }

    @Override
    public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
        onChange.run();
    }

    @Override
    public void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role) {
        onChange.run();
    }

    @Override
    public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
        onChange.run();
    }
}
