/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import java.util.Objects;

/// Decides whether this data node may start new recoveries right now, on top of the concurrency bound enforced by
/// [ThrottlingRecoveryService]. The decision is node-wide (see [Decision]): a gate lets all new recoveries start or holds them all back.
/// The node's gates are combined most-restrictive-wins by [RecoveryGates].
///
/// [#evaluate] is on the recovery dispatch path: it must be fast, non-blocking, and must not call back into recovery scheduling.
public interface RecoveryGate {

    /// Evaluates whether new recoveries may start now on this node.
    Decision evaluate();

    /// Registers a handler the gate invokes whenever its [#evaluate] decision may have changed, so the scheduler re-checks promptly. A
    /// gate whose decision depends on an external signal MUST invoke it when that signal changes.
    /// This handler should never be invoked by [#evaluate].
    default void setGateChangeHandler(Runnable gateChangeHandler) {}

    /// The outcome of evaluating a [RecoveryGate].
    ///
    /// @param mayRun   whether new recoveries may start now; `false` holds back every queued recovery
    /// @param gateName the gate responsible for a block, empty when `mayRun`; safe as a metric attribute (low cardinality)
    /// @param reason   human-readable explanation for logging only, empty when `mayRun`; never a metric attribute (high cardinality)
    record Decision(boolean mayRun, String gateName, String reason) {

        /// Shared "may start now" decision (no gate responsible).
        public static final Decision RUN = new Decision(true, "", "");

        public Decision {
            Objects.requireNonNull(gateName, "gateName");
            Objects.requireNonNull(reason, "reason");
        }

        public static Decision block(String gateName, String reason) {
            return new Decision(false, gateName, reason);
        }
    }
}
