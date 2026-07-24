/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

/// The data node's registered [RecoveryGate]s, combined into one node-wide decision and monitored for transitions.
///
/// Used by [ThrottlingRecoveryService] via [#check]: it aggregates the gates most-restrictive-wins, reports each blocked ↔ may-run
/// transition through the block/unblock callbacks, and returns the current decision. A gate signals a possible change through the
/// re-check handler forwarded to it on [#addGate], which re-runs [#check]. Thread-safe.
final class RecoveryGates {

    private final List<RecoveryGate> gates = new CopyOnWriteArrayList<>();

    /// Forwarded to each gate on [#addGate] as its change handler; invoking it asks the scheduler to re-check.
    private final Runnable onGateChange;
    private final BiConsumer<String, String> onBlocked;
    private final ObjLongConsumer<String> onUnblocked;
    private final LongSupplier relativeTimeInMillisSupplier;

    /// The decision at the last transition (initially may-run); its `mayRun()` is the current state. Guarded by `this`.
    private RecoveryGate.Decision lastTransitionDecision = RecoveryGate.Decision.RUN;

    /// Monotonic time (millis) of the last transition, to measure how long the node stayed blocked. Guarded by `this`.
    private long lastTransitionMillis;

    /// @param onGateChange re-check handler forwarded to each gate as its change handler
    /// @param onBlocked    invoked with the gate's name and reason when the node starts holding recoveries back
    /// @param onUnblocked  invoked with the gate's name and how long the node was blocked, when it resumes
    RecoveryGates(
        Runnable onGateChange,
        BiConsumer<String, String> onBlocked,
        ObjLongConsumer<String> onUnblocked,
        LongSupplier relativeTimeInMillisSupplier
    ) {
        this.onGateChange = onGateChange;
        this.onBlocked = onBlocked;
        this.onUnblocked = onUnblocked;
        this.relativeTimeInMillisSupplier = relativeTimeInMillisSupplier;
        this.lastTransitionMillis = relativeTimeInMillisSupplier.getAsLong();
    }

    void addGate(RecoveryGate gate) {
        gates.add(gate);
        gate.setGateChangeHandler(onGateChange);
    }

    // visible for testing
    boolean isEmpty() {
        return gates.isEmpty();
    }

    /// Aggregates the gates, reports any transition via the block/unblock callbacks, and returns the current decision.
    RecoveryGate.Decision check() {
        final RecoveryGate.Decision current;
        final RecoveryGate.Decision previous;
        final long blockedTimeMillis;
        synchronized (this) {
            current = evaluate();
            if (current.mayRun() == lastTransitionDecision.mayRun()) {
                return current; // no transition
            }
            previous = lastTransitionDecision;
            final long now = relativeTimeInMillisSupplier.getAsLong();
            blockedTimeMillis = now - lastTransitionMillis;
            lastTransitionDecision = current;
            lastTransitionMillis = now;
        }
        // Flipped to may-run => just unblocked (previous held the block); else newly blocked.
        if (current.mayRun()) {
            onUnblocked.accept(previous.gateName(), blockedTimeMillis);
        } else {
            onBlocked.accept(current.gateName(), current.reason());
        }
        return current;
    }

    /// How long the node has currently been blocked, in millis, or 0 if not currently blocked.
    synchronized long blockedDurationMillis() {
        return lastTransitionDecision.mayRun() ? 0L : relativeTimeInMillisSupplier.getAsLong() - lastTransitionMillis;
    }

    /// The current node-wide decision, most-restrictive-wins: the first non-may-run gate's decision, else [RecoveryGate.Decision#RUN].
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
