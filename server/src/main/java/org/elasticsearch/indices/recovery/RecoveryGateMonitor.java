/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/// Monitors the data node's [RecoveryGate]s, combining them most-restrictive-wins into one node-wide decision.
///
/// [#evaluate] returns the current decision. [#addListener] registers a one-shot listener fired once the decision evaluates to the
/// awaited outcome; while any listener waits, the gates are re-evaluated periodically so the awaited outcome is noticed without an
/// external trigger. Thread-safe.
public final class RecoveryGateMonitor {

    /// How often to re-evaluate the gates while a listener is waiting.
    // TODO: make this configurable via a node setting
    private static final TimeValue RECHECK_INTERVAL = TimeValue.timeValueSeconds(1);

    /// Resolves the node's gates once, on first use, since plugin-contributed gates only exist late in node construction.
    private final Supplier<List<RecoveryGate>> gates;
    private final ThreadPool threadPool;

    /// One-shot listeners awaiting an outcome, keyed by caller-chosen name (one listener per name), fired and cleared by a [#check]
    /// that evaluates to that outcome. Guarded by `this`.
    private final Map<RecoveryGate.Outcome, Map<String, Runnable>> listeners = new EnumMap<>(RecoveryGate.Outcome.class);

    /// Whether a recheck is scheduled; at most one is pending at a time. Guarded by `this`.
    private boolean recheckScheduled;

    public RecoveryGateMonitor(Supplier<Collection<RecoveryGate>> gatesSupplier, ThreadPool threadPool) {
        this.gates = CachedSupplier.wrap(() -> List.copyOf(gatesSupplier.get()));
        this.threadPool = threadPool;
    }

    /// The current node-wide decision, most-restrictive-wins: the first non-may-run gate's decision, else [RecoveryGate.Decision#RUN].
    public RecoveryGate.Decision evaluate() {
        for (RecoveryGate gate : gates.get()) {
            final RecoveryGate.Decision decision = gate.evaluate();
            if (decision.mayRun() == false) {
                return decision;
            }
        }
        return RecoveryGate.Decision.RUN;
    }

    /// Registers a one-shot listener fired once the decision evaluates to `awaitedOutcome`, or inline if it already does (so a caller
    /// racing with a change cannot miss it). While a listener waits, the gates are re-evaluated periodically. Listeners are keyed by
    /// caller-chosen `name`, so independent consumers do not override each other; re-registering a name already awaiting the outcome
    /// is a no-op.
    public void addListener(RecoveryGate.Outcome awaitedOutcome, String name, Runnable listener) {
        synchronized (this) {
            listeners.computeIfAbsent(awaitedOutcome, outcome -> new LinkedHashMap<>()).putIfAbsent(name, listener);
        }
        check(); // evaluate immediately: fires the listener inline if the decision already matches, else schedules the recheck
    }

    /// Evaluates the gates, fires the listeners awaiting the evaluated outcome (off the lock), and schedules a recheck while any
    /// listener remains.
    private void check() {
        final List<Runnable> awaitingOnDecision;
        synchronized (this) {
            awaitingOnDecision = drainListeners(evaluate().outcome());
            if (listeners.isEmpty() == false) {
                scheduleRecheck();
            }
        }
        awaitingOnDecision.forEach(Runnable::run);
    }

    /// Retrieves and removes the listeners waiting for the given outcome, in registration order.
    private List<Runnable> drainListeners(RecoveryGate.Outcome outcome) {
        assert Thread.holdsLock(this);
        final Map<String, Runnable> awaiting = listeners.remove(outcome);
        return awaiting == null ? List.of() : List.copyOf(awaiting.values());
    }

    private void scheduleRecheck() {
        assert Thread.holdsLock(this);
        if (recheckScheduled == false) {
            recheckScheduled = true;
            threadPool.schedule(() -> {
                synchronized (this) {
                    recheckScheduled = false;
                }
                check();
            }, RECHECK_INTERVAL, threadPool.generic());
        }
    }
}
