/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;

/**
 * Node-wide ticket gate for retained external-source I/O bytes. Look-ahead uses {@link #tryAdmit};
 * a unit that must proceed uses {@link #admitAsync}. One overshoot slot exists per node, granted
 * only to a runnable lease; a unit larger than the cap goes through that slot only. Peak occupancy
 * is the cap plus one unit, not one unit per query. Unconditional {@link #add} (ungated
 * alloc) can sit besides that bound.
 * <p>
 * {@link Hold#close()} is idempotent and drops leftover byte charge only. It does
 * not clear the overshoot owner: buffers force-added beside the hold still occupy
 * {@link #used()}, and a second over-cap unit must wait until {@link #clearOwner}.
 * A grant that lands on a cancelled waiter is released immediately. Uncontended grants
 * complete on the caller; contended grants are forked onto the executor supplied to
 * {@link #admitAsync}. Charge helpers ({@link #add}, {@link #release}, {@link #clearOwner})
 * stay on this type because look-ahead and ungated alloc share the same cap. Tickets
 * use {@link #tryAdmit}, {@link #admitAsync}, {@link Hold}, and {@link #wakeWaiters}.
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
     * cancel. Uncontended grants complete on the caller; contended grants are forked onto
     * {@code executor}. {@code lease} is required when the unit needs the overshoot slot;
     * {@code cancelSignal} is sampled at enqueue and at grant.
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
     * Does not release bytes. Signals waiters. Call this when the lease is done; {@link Hold#close()}
     * does not, because sliding-window buffers may still sit in {@link #used()}.
     */
    void clearOwner(RowGroupIo lease);

    @Nullable
    RowGroupIo overshootOwner();

    long used();

    long limit();

    /**
     * Fails waiters whose cancel signal is set and grants the next runnable waiter. Installed as
     * {@link RowGroupIo#setWake} so lease cancel is prompt.
     */
    void wakeWaiters();

    /**
     * Failure for a cancelled byte-ticket waiter. Shared so callers do not import the service
     * implementation.
     */
    static EsRejectedExecutionException cancelled() {
        return new EsRejectedExecutionException("Cancelled while waiting for I/O bytes");
    }

    /**
     * Reservation of {@link #tryAdmit} / {@link #admitAsync} bytes. {@link #drop(long)} swaps
     * leftover estimate for a real alloc; {@link #close()} clears any remainder. Idempotent.
     * {@link #close()} does not {@link NodeByteBudget#clearOwner}.
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

        /**
         * Releases leftover estimate. Does not {@link NodeByteBudget#clearOwner}: overshoot
         * occupancy can outlive this hold while force-added buffers remain.
         */
        @Override
        void close();
    }
}
