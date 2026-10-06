/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Node-wide ticket gate for retained external-source I/O bytes. Look-ahead uses {@link #tryAdmit};
 * a unit that must proceed uses {@link #admitAsync}. One overshoot slot exists per node, granted
 * only to a runnable lease; a unit larger than the cap goes through that slot only. Peak occupancy
 * is the cap plus one unit, not one unit per query.
 * <p>
 * {@link Hold#close()} is idempotent. A grant that lands on a cancelled waiter is released
 * immediately. Grant callbacks run on the executor supplied to {@link #admitAsync}, not inline on
 * the releasing thread.
 */
public interface NodeByteBudget {

    /**
     * Non-blocking reservation. Returns a hold when {@code used + bytes} fits under the cap with
     * no ticket waiters queued; otherwise {@code null}. Does not take the overshoot slot.
     * {@code bytes == 0} always succeeds.
     */
    @Nullable
    Hold tryAdmit(long bytes);

    /**
     * FIFO ticket for {@code bytes}. Completes with a {@link Hold} on grant, or with failure on
     * cancel. The grant is forked onto {@code executor}. {@code lease} is required when the unit
     * needs the overshoot slot; {@code cancelSignal} is sampled at enqueue and at grant.
     */
    SubscribableListener<Hold> admitAsync(long bytes, RowGroupIo lease, BooleanSupplier cancelSignal, Executor executor);

    /**
     * Unconditional charge for buffers that must exist (UNGATED alloc, sliding window). Not a
     * ticket and not a decode-shortfall reconcile.
     */
    void add(long bytes);

    /** Drops {@code bytes} of charge and grants the next waiter when there is room. */
    void release(long bytes);

    /**
     * Drops {@code lease} as the node-wide overshoot owner if it currently holds that slot.
     * Does not release bytes. Signals waiters.
     */
    void clearOwner(RowGroupIo lease);

    @Nullable
    RowGroupIo overshootOwner();

    long used();

    long limit();

    /**
     * Highest {@link #used()} observed, including the one overshoot unit. Tests use this for the
     * cap+1-unit peak bound.
     */
    long peakUsed();

    /** Queued {@link #admitAsync} waiters. Tests assert no leak. */
    int waiterCount();

    /**
     * Fails waiters whose cancel signal is set and grants the next runnable waiter. Installed as
     * {@link RowGroupIo#setWake} so lease cancel is prompt.
     */
    void wakeWaiters();

    /**
     * Reservation of {@link #tryAdmit} / {@link #admitAsync} bytes. {@link #drop(long)} swaps
     * leftover estimate for a real alloc; {@link #close()} clears any remainder. Idempotent.
     */
    interface Hold extends Releasable {

        long bytes();

        long remaining();

        @Nullable
        RowGroupIo lease();

        boolean isOvershoot();

        /**
         * Releases up to {@code bytes} of leftover estimate so a real buffer can be charged
         * beside it. Sibling in-flight ranges keep their estimate.
         */
        void drop(long bytes);

        @Override
        void close();
    }
}
