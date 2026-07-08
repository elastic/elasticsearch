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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/// A mutable composite [RecoverySchedulingListener] that fans out every event to all registered subscribers.
///
/// Subscribers are added and removed dynamically via [#addListener] and [#removeListener].
public class CompositeRecoverySchedulingListener implements RecoverySchedulingListener {

    private final List<RecoverySchedulingListener> listeners = new CopyOnWriteArrayList<>();

    public void addListener(RecoverySchedulingListener listener) {
        listeners.add(listener);
    }

    public void removeListener(RecoverySchedulingListener listener) {
        listeners.remove(listener);
    }

    @Override
    public void onRecoveryQueued(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryQueued(type, role);
        }
    }

    @Override
    public void onQueuedRecoveryDiscarded(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onQueuedRecoveryDiscarded(type, role);
        }
    }

    @Override
    public void onRecoveryStarted(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryStarted(type, role);
        }
    }

    @Override
    public void onRecoveryDequeuedAndStarted(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryDequeuedAndStarted(type, role);
        }
    }

    @Override
    public void onStartedRecoveryCancelled(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onStartedRecoveryCancelled(type, role);
        }
    }

    @Override
    public void onRecoveryCompleted(RecoverySource.Type type, RecoveryRole role) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryCompleted(type, role);
        }
    }
}
