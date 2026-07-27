/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import java.util.Collection;
import java.util.List;
import java.util.function.LongSupplier;

/// The data node's registered [RecoveryGate]s, combined into one node-wide decision and monitored for transitions.
///
/// Used by [ThrottlingRecoveryService] via [#check]: it aggregates the gates most-restrictive-wins, reports each blocked ↔ may-run
/// transition through the block/unblock callbacks, and returns the current decision. A gate signals a possible change through the
/// re-check handler, which may re-run [#check]. Thread-safe.
final class RecoveryGates {

    /// Notified when the aggregate decision transitions. `previousStateDurationMillis` is how long the node stayed in `previous`.
    @FunctionalInterface
    interface DecisionChangeListener {
        void onChange(RecoveryGate.Decision previous, RecoveryGate.Decision current, long previousStateDurationMillis);
    }

    private final List<RecoveryGate> gates;
    private final DecisionChangeListener onChange;
    private final LongSupplier relativeTimeInMillisSupplier;

    /// The decision at the last transition (initially RUN); its outcome is the current state. Guarded by `this`.
    private RecoveryGate.Decision lastTransitionDecision = RecoveryGate.Decision.RUN;

    /// Monotonic time (millis) of the last transition, to measure how long the node stayed in that state. Guarded by `this`.
    private long lastTransitionMillis;

    /// @param gates        the node's recovery gates, fixed for the lifetime of this instance
    /// @param onGateChange set on each gate as its change handler; a gate invokes it to ask the scheduler to re-check
    /// @param onChange     notified when the aggregate decision transitions (blocked ↔ may-run)
    RecoveryGates(
        Collection<RecoveryGate> gates,
        Runnable onGateChange,
        DecisionChangeListener onChange,
        LongSupplier relativeTimeInMillisSupplier
    ) {
        this.gates = List.copyOf(gates);
        this.onChange = onChange;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
        this.lastTransitionMillis = relativeTimeInMillisSupplier.getAsLong();
        this.gates.forEach(gate -> gate.setGateChangeHandler(onGateChange));
    }

    // visible for testing
    boolean isEmpty() {
        return gates.isEmpty();
    }

    /// Aggregates the gates and, if the decision transitioned since the last call, reports it to the [DecisionChangeListener], and returns
    /// the current decision.
    RecoveryGate.Decision check() {
        final RecoveryGate.Decision current;
        final RecoveryGate.Decision previous;
        final long previousStateDurationMillis;
        synchronized (this) {
            current = evaluate();
            if (current.mayRun() == lastTransitionDecision.mayRun()) {
                return current; // no transition
            }
            previous = lastTransitionDecision;
            final long now = relativeTimeInMillisSupplier.getAsLong();
            previousStateDurationMillis = now - lastTransitionMillis;
            lastTransitionDecision = current;
            lastTransitionMillis = now;
        }
        onChange.onChange(previous, current, previousStateDurationMillis);
        return current;
    }

    /// How long the node has currently been blocked, in millis, or 0 if not currently blocked.
    synchronized long blockedDurationMillis() {
        return lastTransitionDecision.mayRun() ? 0L : relativeTimeInMillisSupplier.getAsLong() - lastTransitionMillis;
    }

    // visible for testing
    RecoveryGate.Decision evaluate() {
        for (RecoveryGate gate : gates) {
            final RecoveryGate.Decision decision = gate.evaluate();
            if (decision.mayRun() == false) {
                return decision;
            }
        }
        return RecoveryGate.Decision.RUN;
    }
}
