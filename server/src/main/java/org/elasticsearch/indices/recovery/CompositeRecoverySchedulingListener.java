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
    public void onRecoveryCancelledBeforeQueuingOnTarget(RecoverySource.Type type) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryCancelledBeforeQueuingOnTarget(type);
        }
    }

    @Override
    public void onRecoveryQueuedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryQueuedOnTarget(type, priorityGroup);
        }
    }

    @Override
    public void onPeerRecoveryQueuedOnSource() {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onPeerRecoveryQueuedOnSource();
        }
    }

    @Override
    public void onQueuedRecoveryDiscardedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onQueuedRecoveryDiscardedOnTarget(type, priorityGroup);
        }
    }

    @Override
    public void onQueuedPeerRecoveryDiscardedOnSource() {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onQueuedPeerRecoveryDiscardedOnSource();
        }
    }

    @Override
    public void onQueuedRecoveryCancelledOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onQueuedRecoveryCancelledOnTarget(type, priorityGroup);
        }
    }

    @Override
    public void onPeerRecoveryStartedOnSource() {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onPeerRecoveryStartedOnSource();
        }
    }

    @Override
    public void onRecoveryDequeuedAndStartedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryDequeuedAndStartedOnTarget(type, priorityGroup);
        }
    }

    @Override
    public void onPeerRecoveryDequeuedAndStartedOnSource() {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onPeerRecoveryDequeuedAndStartedOnSource();
        }
    }

    @Override
    public void onStartedRecoveryCancelledOnTarget(RecoverySource.Type type) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onStartedRecoveryCancelledOnTarget(type);
        }
    }

    @Override
    public void onRecoveryCompletedOnTarget(RecoverySource.Type type, PriorityGroup priorityGroup) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveryCompletedOnTarget(type, priorityGroup);
        }
    }

    @Override
    public void onPeerRecoveryCompletedOnSource() {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onPeerRecoveryCompletedOnSource();
        }
    }

    @Override
    public void onRecoveriesBlocked(String gateName) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveriesBlocked(gateName);
        }
    }

    @Override
    public void onRecoveriesUnblocked(long blockedTimeMillis) {
        for (RecoverySchedulingListener listener : listeners) {
            listener.onRecoveriesUnblocked(blockedTimeMillis);
        }
    }
}
