/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

/**
 * Per-query grant and overshoot-pin policy for {@link RowGroupIo} leases. The production
 * implementation is the package-private query budget; tests bind a fake through
 * {@link RowGroupIo#attachScheduler}.
 */
public interface RowGroupScheduler {

    /**
     * Attempts to pin {@code io} as the query's sole overshoot owner and favourite. Succeeds
     * unless {@code io} is null or finished, the scheduler is closed, or another unfinished
     * lease already holds the pin. A second pin while one is live returns {@code false}.
     */
    boolean tryPinOvershoot(RowGroupIo io);

    /** Clears the overshoot pin if it currently points at {@code io}. */
    void unpin(RowGroupIo io);

    /**
     * Marks {@code io} finished, drops it from the scheduler's registry, and clears a stale
     * favourite/pin that still points at it. Must also unblock any thread blocked in
     * {@code acquire} for this lease — typically by signalling its wait condition so the
     * waiter observes {@link RowGroupIo#isFinished()} and fails the acquire. Implementors
     * that skip this leave those waiters parked until the acquire timeout.
     */
    void finish(RowGroupIo io);
}
