/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/// Monitors the data node's [RecoveryGate]s, combining them most-restrictive-wins into one node-wide decision.
///
/// [#evaluate] returns the current decision. [#addCallback] registers a one-shot callback fired once the decision evaluates to
/// the awaited outcome; while any callback waits, the gates are re-evaluated periodically so the awaited outcome is noticed without an
/// external trigger. Thread-safe.
public final class RecoveryGateMonitor {

    private static final Logger logger = LogManager.getLogger(RecoveryGateMonitor.class);

    public static final Setting<Boolean> ENABLE_RECOVERY_GATES_SETTING = Setting.boolSetting(
        "indices.recovery.gates.enabled",
        false,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /// How often to re-evaluate the gates while a callback is waiting.
    // TODO: make this configurable via a node setting
    private static final TimeValue RECHECK_INTERVAL = TimeValue.timeValueSeconds(1);

    /// Resolves the node's gates once, on first use, since plugin-contributed gates only exist late in node construction.
    private final Supplier<List<RecoveryGate>> gates;
    private final ThreadPool threadPool;

    private volatile boolean gatesEnabled;

    /// One-shot callbacks awaiting an outcome, fired and cleared by a [#check] that evaluates to it. Guarded by `this`.
    private final Map<RecoveryGate.Outcome, List<Runnable>> outcomeCallbacks = new EnumMap<>(RecoveryGate.Outcome.class);

    /// Whether a recheck is scheduled; at most one is pending at a time. Guarded by `this`.
    private boolean recheckScheduled;

    public RecoveryGateMonitor(Supplier<Collection<RecoveryGate>> gatesSupplier, ThreadPool threadPool, ClusterSettings clusterSettings) {
        this.gates = CachedSupplier.wrap(() -> List.copyOf(gatesSupplier.get()));
        this.threadPool = threadPool;
        clusterSettings.initializeAndWatchIfRegistered(ENABLE_RECOVERY_GATES_SETTING, enabled -> this.gatesEnabled = enabled);
    }

    /// The current node-wide decision, most-restrictive-wins: the first blocking gate's decision, else [RecoveryGate.Decision#RUN].
    /// A gate that throws is ignored (failing open, i.e. towards pre-gating behaviour) with a warning, so a buggy gate degrades to no
    /// gating rather than stalling recoveries indefinitely.
    public RecoveryGate.Decision evaluate() {
        if (gatesEnabled == false) {
            return RecoveryGate.Decision.RUN;
        }
        for (RecoveryGate gate : gates.get()) {
            final RecoveryGate.Decision decision;
            try {
                decision = gate.evaluate();
            } catch (Exception e) {
                logger.warn(() -> "recovery gate [" + gate.getClass().getName() + "] failed to evaluate and is ignored", e);
                continue;
            }
            assert decision != null : "recovery gate [" + gate.getClass().getName() + "] returned null decision";
            if (decision.mayRun() == false) {
                return decision;
            }
        }
        return RecoveryGate.Decision.RUN;
    }

    /// Registers a one-shot callback fired once the decision evaluates to `awaitedOutcome`. The gates are re-evaluated so a decision
    /// that already matches cannot be missed, and then periodically while any callback waits. If node is shutting down, the callback
    /// may not be run.
    public void addCallback(RecoveryGate.Outcome awaitedOutcome, Runnable callback) {
        synchronized (this) {
            outcomeCallbacks.computeIfAbsent(awaitedOutcome, outcome -> new ArrayList<>()).add(callback);
        }
        // Evaluate on the generic pool, so registering callback is not blocked on other consumers' callbacks.
        threadPool.generic().execute(this::check);
    }

    /// Evaluates the gates, fires the callbacks awaiting the evaluated outcome, and schedules a recheck while any callback remains.
    private void check() {
        final List<Runnable> callbacks;
        synchronized (this) {
            callbacks = drainCallbacks(evaluate().outcome());
            if (outcomeCallbacks.isEmpty() == false) {
                scheduleRecheck();
            }
        }
        // sequential for now, we could fork and wrap the callbacks in an AbstractRunnable to log failures
        for (Runnable callback : callbacks) {
            try {
                callback.run();
            } catch (Exception e) {
                logger.warn("recovery gate outcome callback failed", e);
            }
        }
    }

    /// Retrieves and removes the callbacks waiting for the given outcome, in registration order.
    private List<Runnable> drainCallbacks(RecoveryGate.Outcome outcome) {
        assert Thread.holdsLock(this);
        final List<Runnable> callbacks = outcomeCallbacks.remove(outcome);
        return callbacks == null ? List.of() : callbacks;
    }

    private void scheduleRecheck() {
        assert Thread.holdsLock(this);
        if (recheckScheduled == false) {
            recheckScheduled = true;
            threadPool.scheduleUnlessShuttingDown(RECHECK_INTERVAL, threadPool.generic(), () -> {
                synchronized (this) {
                    recheckScheduled = false;
                }
                check();
            });
        }
    }
}
