/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * One request-breaker reservation for a query's external planning. Created from the session's block
 * factory so every admit and the final release use that same breaker.
 * <p>
 * Listing and schema-map bytes live until {@link #close()}, which the query listener calls once.
 * Phase-2 bytes belong to a {@link Run}: each compute execution opens one and closes it when that
 * execution finishes, so a later INLINE STATS run does not keep the previous run's split shells reserved.
 * Concurrent runs (UNION siblings) each hold their own {@link Run}.
 */
public final class ExternalPlanningReservation implements Releasable {

    private final CircuitBreaker breaker;
    private final AtomicLong queryHeld = new AtomicLong();
    private final ConcurrentLinkedQueue<Run> runs = new ConcurrentLinkedQueue<>();
    private final AtomicBoolean closed = new AtomicBoolean();

    public ExternalPlanningReservation(CircuitBreaker breaker) {
        this.breaker = breaker;
    }

    /** Listing plus schema-map bytes. Held until {@link #close()}. A trip leaves {@link #queryHeld()} unchanged. */
    public void chargeQuery(long bytes) {
        admit(queryHeld, bytes);
    }

    public long queryHeld() {
        return queryHeld.get();
    }

    /** A phase-2 reservation for one compute execution. */
    public Run openRun() {
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
        refund(queryHeld);
    }

    private void admit(AtomicLong held, long bytes) {
        if (bytes <= 0) {
            return;
        }
        breaker.addEstimateBytesAndMaybeBreak(bytes, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        held.addAndGet(bytes);
    }

    private void refund(AtomicLong held) {
        long bytes = held.getAndSet(0);
        if (bytes > 0) {
            breaker.addWithoutBreaking(-bytes, EsqlExecutionInfo.EXTERNAL_PLANNING_LABEL);
        }
    }

    /**
     * Phase-2 bytes for one compute execution. {@link #close()} is idempotent, so the execution
     * listener and {@link ExternalPlanningReservation#close()} can both release it.
     */
    public final class Run implements Releasable {
        private final AtomicLong held = new AtomicLong();
        private final AtomicBoolean released = new AtomicBoolean();

        /**
         * Survivor-map and split-shell bytes. A trip leaves {@link #held()} unchanged: the breaker
         * throws before the add.
         */
        public void charge(long bytes) {
            admit(held, bytes);
        }

        public long held() {
            return held.get();
        }

        @Override
        public void close() {
            if (released.compareAndSet(false, true) == false) {
                return;
            }
            refund(held);
        }
    }
}
