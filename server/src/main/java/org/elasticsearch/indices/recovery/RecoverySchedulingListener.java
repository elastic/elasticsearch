/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

/// Listener for recovery scheduling changes. Invoked when a recovery starts, end, or is queued/dequeued.
/// TODO: This and RecoveryStats should also be able to track local/snapshot recoveries
@FunctionalInterface
public interface RecoverySchedulingListener {

    /// Invoked after recovery scheduling changes due to recovery lifecycle events
    /// Implementations must be thread-safe, should not block or throw any exceptions.
    void onRecoverySchedulingChange();
}
