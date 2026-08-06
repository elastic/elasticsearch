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
/// The node's gates are combined most-restrictive-wins by [RecoveryGateMonitor].
///
/// [#evaluate] is on the recovery dispatch path: it must be fast, non-blocking, and must not call back into recovery scheduling.
@FunctionalInterface
public interface RecoveryGate {

    /// Evaluates whether new recoveries may start now on this node. Must not throw: a throwing gate is ignored by the
    /// [RecoveryGateMonitor].
    Decision evaluate();

    enum Outcome {
        RUN,   // new recoveries may start
        BLOCK  // new recoveries are all blocked
    }

    /// The outcome of evaluating a [RecoveryGate].
    ///
    /// @param outcome  whether new recoveries may all start ([Outcome#RUN]) or are all held back ([Outcome#BLOCK])
    /// @param gateName the blocking gate; `"ALL"` for a run decision. Safe as a metric attribute (low cardinality)
    /// @param reason   human-readable explanation, for logging only; never a metric attribute (high cardinality)
    record Decision(Outcome outcome, String gateName, String reason) {
        /// Shared "may start now" decision
        public static final Decision RUN = new Decision(Outcome.RUN, "ALL", "All gates pass");

        public static Decision block(String gateName, String reason) {
            return new Decision(Outcome.BLOCK, gateName, reason);
        }

        public Decision {
            Objects.requireNonNull(outcome, "outcome");
            Objects.requireNonNull(gateName, "gateName");
            Objects.requireNonNull(reason, "reason");
        }

        /// Whether new recoveries may start now.
        public boolean mayRun() {
            return outcome == Outcome.RUN;
        }
    }
}
