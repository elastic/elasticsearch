/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalPlanningIo;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One request-breaker reservation for a query's external planning. Created from the session's block
 * factory so every admit and the final release use that same breaker.
 * <p>
 * Resolution's listing and the schema map live until {@link #close()}, which the query listener calls once,
 * because the plan holds both for as long as the query does.
 * A {@link Run} holds bytes released before that: phase-2 split shells for one compute execution, the listing
 * split discovery performs for itself in that execution, and the private attribute lists of one reconcile
 * gather. Each compute execution closes its run when that execution
 * finishes, so a later INLINE STATS run does not keep the previous run's split shells reserved. The gather
 * closes its run when its completion drops those lists. Concurrent runs (UNION siblings) each hold their own.
 */
public final class ExternalPlanningReservation implements Releasable {

    private final CircuitBreaker breaker;
    private final AtomicLong queryHeld = new AtomicLong();
    private final ConcurrentLinkedQueue<Run> runs = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final ExternalPlanningIo planningIo = new ExternalPlanningIo();

    public ExternalPlanningReservation(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /** Query-scoped received-byte / request tally for coordinator planning I/O. */
    public ExternalPlanningIo planningIo() {
        return planningIo;
    }

    /** Listing plus schema-map bytes. Held until {@link #close()}. A trip leaves {@link #queryHeld()} unchanged. */
    public void chargeQuery(long bytes) {
        admit(queryHeld, bytes, closed, "query");
    }

    public long queryHeld() {
        return queryHeld.get();
    }

    /** One compute execution, or one reconcile gather. */
    public Run openRun() {
        refuseAfterRelease(closed, "query");
        Run run = new Run();
        runs.add(run);
        return run;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) == false) {
            return;
        }
        for (Run run : runs) {
            run.close();
        }
        refund(queryHeld, closed);
    }

    private void admit(AtomicLong held, long bytes, AtomicBoolean releasedAlready, String scope) {
        if (bytes <= 0) {
            return;
        }
        // Charged before the flag is read, so a trip costs nothing and leaves nothing held. What follows decides
        // whether we get to keep it.
        breaker.addEstimateBytesAndMaybeBreak(bytes, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        boolean kept = false;
        synchronized (releasedAlready) {
            if (releasedAlready.get() == false) {
                held.addAndGet(bytes);
                kept = true;
            }
        }
        if (kept == false) {
            // The reservation was released while this charge was in flight. Reading the flag and then adding would
            // have let the refund run between the two and take a total that does not include these bytes, leaving
            // them on the request breaker for the node's lifetime - nothing releases twice. Hand them back here,
            // then refuse, so the caller still learns it charged too late.
            breaker.addWithoutBreaking(-bytes, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
            throw new IllegalStateException("external planning memory cannot be reserved against a closed " + scope + " reservation");
        }
    }

    /**
     * Refuses to open a run against a reservation that has already refunded.
     * <p>
     * {@link #close()} closes every run it knows about and then sets the held total back to zero; a run opened
     * after that is on nobody's close path, so whatever it charges is never released. Nothing closes twice.
     * <p>
     * A late <em>charge</em> is handled in {@link #admit} instead, which cannot use this check on its own: reading
     * a flag and then adding leaves a window for the refund to run between the two.
     */
    private static void refuseAfterRelease(AtomicBoolean releasedAlready, String scope) {
        if (releasedAlready.get()) {
            throw new IllegalStateException("external planning memory cannot be reserved against a closed " + scope + " reservation");
        }
    }

    private void refund(AtomicLong held, AtomicBoolean releasedAlready) {
        long bytes;
        // Under the same lock admit takes, so a charge is either counted in this total or refuses and returns its
        // own bytes. The flag is already set by the caller, which is what makes the second outcome safe.
        synchronized (releasedAlready) {
            bytes = held.getAndSet(0);
        }
        if (bytes > 0) {
            breaker.addWithoutBreaking(-bytes, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        }
    }

    /**
     * Bytes released before query close: phase-2 split shells for one compute execution, the listing that
     * execution's split discovery performed for itself, or the private attribute lists of one reconcile
     * gather. {@link #close()} is idempotent, so the owner and
     * {@link ExternalPlanningReservation#close()} can both release it.
     */
    public final class Run implements Releasable {
        private final AtomicLong held = new AtomicLong();
        private final AtomicBoolean released = new AtomicBoolean();

        /**
         * Survivor-map, split-shell, discovered-listing, or private schema-list bytes. A trip leaves
         * {@link #held()} unchanged:
         * the breaker throws before the add.
         */
        public void charge(long bytes) {
            admit(held, bytes, released, "run");
        }

        public long held() {
            return held.get();
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true) == false) {
                return;
            }
            refund(held, released);
        }
    }
}
