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
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/// Monitors the data node's [RecoveryGate]s, combining them most-restrictive-wins into one node-wide decision.
///
/// [#check] evaluates the gates and, when the decision transitions (blocked ↔ may-run), notifies the [DecisionChangeListener] and
/// fires any listener registered via [#addListener]. While a listener waits, the gates are re-evaluated periodically so the awaited
/// change is noticed without an external trigger. Thread-safe.
public final class RecoveryGateMonitor {

    /// How often to re-evaluate the gates while a listener is waiting for the decision to change.
    // TODO: make this configurable via a node setting
    private static final TimeValue RECHECK_INTERVAL = TimeValue.timeValueSeconds(1);

    /// Notified when the aggregate decision transitions. `previousStateDurationMillis` is how long the node stayed in `previous`.
    @FunctionalInterface
    public interface DecisionChangeListener {
        DecisionChangeListener NOOP = (previous, current, previousStateDurationMillis) -> {};

        void onDecisionChange(RecoveryGate.Decision previous, RecoveryGate.Decision current, long previousStateDurationMillis);
    }

    /// Resolves the node's gates once, on first use.
    private final Supplier<List<RecoveryGate>> gates;
    /// Resolves the change listener once, on the first transition.
    private final Supplier<DecisionChangeListener> onChange;
    private final ThreadPool threadPool;

    /// The decision at the last transition (initially RUN); its outcome is the current state. Guarded by `this`.
    private RecoveryGate.Decision lastTransitionDecision = RecoveryGate.Decision.RUN;

    /// Monotonic time (millis) of the last transition, to measure how long the node stayed in that state. Guarded by `this`.
    private long lastTransitionMillis;

    /// One-shot listeners awaiting an outcome, keyed by caller-chosen name (one listener per name), fired and cleared when a [#check]
    /// transitions to it. Guarded by `this`.
    private final Map<RecoveryGate.Outcome, Map<String, Runnable>> listeners = new EnumMap<>(RecoveryGate.Outcome.class);

    /// The periodic re-evaluation, scheduled while any listener is waiting (i.e. `listeners` is non-empty). Guarded by `this`.
    private Scheduler.Cancellable recheckTask;

    /// Both suppliers are resolved lazily (once, on first use), so they may refer to components created after this instance, e.g.
    /// plugin-contributed gates or the recovery scheduler this instance is injected into.
    ///
    /// @param gatesSupplier    resolves the node's recovery gates; called once
    /// @param onChangeSupplier resolves the listener notified when the aggregate decision transitions; called once, on the first
    ///                          transition
    public RecoveryGateMonitor(
        Supplier<Collection<RecoveryGate>> gatesSupplier,
        Supplier<DecisionChangeListener> onChangeSupplier,
        ThreadPool threadPool
    ) {
        this.gates = CachedSupplier.wrap(() -> List.copyOf(gatesSupplier.get()));
        this.onChange = CachedSupplier.wrap(onChangeSupplier);
        this.threadPool = threadPool;
        this.lastTransitionMillis = threadPool.relativeTimeInMillis();
    }

    /// Evaluates the gates and returns the current decision. On a transition, notifies the [DecisionChangeListener], fires the
    /// listener awaiting the new outcome, and stops the periodic recheck once no listener remains. Notifications run off the lock, so
    /// under rapid flapping they may arrive out of order (durations stay accurate) — consumers must not derive current state from them.
    RecoveryGate.Decision check() {
        final RecoveryGate.Decision current;
        final RecoveryGate.Decision previous;
        final long previousStateDurationMillis;
        final List<Runnable> awaitingOnDecision;
        final DecisionChangeListener changeListener;
        synchronized (this) {
            current = evaluate();
            if (current.outcome() == lastTransitionDecision.outcome()) {
                return current; // no transition
            }
            changeListener = onChange.get();
            previous = lastTransitionDecision;
            final long now = threadPool.relativeTimeInMillis();
            previousStateDurationMillis = now - lastTransitionMillis;
            lastTransitionDecision = current;
            lastTransitionMillis = now;
            awaitingOnDecision = drainListeners(current.outcome());
            // no more listeners, stop the scheduled recheck
            if (listeners.isEmpty()) {
                stopRecheck();
            }
        }
        try {
            changeListener.onDecisionChange(previous, current, previousStateDurationMillis);
        } finally {
            // always fire the listeners waiting on decision
            awaitingOnDecision.forEach(Runnable::run);
        }
        return current;
    }

    /// Registers a one-shot listener fired once the decision transitions to `awaitedOutcome`, or inline if it already matches (so a
    /// caller racing with a transition cannot miss it). A waiting listener starts the periodic recheck. Listeners are keyed by
    /// caller-chosen `name`, so independent consumers do not override each other; re-registering a name already awaiting the outcome
    /// is a no-op.
    void addListener(RecoveryGate.Outcome awaitedOutcome, String name, Runnable listener) {
        synchronized (this) {
            if (lastTransitionDecision.outcome() != awaitedOutcome) {
                listeners.computeIfAbsent(awaitedOutcome, outcome -> new LinkedHashMap<>()).putIfAbsent(name, listener);
                startRecheck();
                return;
            }
        }
        listener.run();
    }

    /// How long the node has currently been blocked, in millis, or 0 if not currently blocked.
    synchronized long blockedDurationMillis() {
        return lastTransitionDecision.mayRun() ? 0L : threadPool.relativeTimeInMillis() - lastTransitionMillis;
    }

    /// The current node-wide decision, most-restrictive-wins: the first non-may-run gate's decision, else [RecoveryGate.Decision#RUN].
    /// Reads no mutable state; [#check] calls it under the lock only to keep evaluate-and-record atomic.
    // visible for testing
    RecoveryGate.Decision evaluate() {
        for (RecoveryGate gate : gates.get()) {
            final RecoveryGate.Decision decision = gate.evaluate();
            if (decision.mayRun() == false) {
                return decision;
            }
        }
        return RecoveryGate.Decision.RUN;
    }

    /// Retrieves and removes the listeners waiting for the given outcome
    private List<Runnable> drainListeners(RecoveryGate.Outcome outcome) {
        assert Thread.holdsLock(this);
        final Map<String, Runnable> awaiting = listeners.remove(outcome);
        return awaiting == null ? List.of() : List.copyOf(awaiting.values());
    }

    private void startRecheck() {
        assert Thread.holdsLock(this);
        if (recheckTask == null) {
            recheckTask = threadPool.scheduleWithFixedDelay(this::check, RECHECK_INTERVAL, threadPool.generic());
        }
    }

    private void stopRecheck() {
        assert Thread.holdsLock(this);
        if (recheckTask != null) {
            recheckTask.cancel();
            recheckTask = null;
        }
    }
}
