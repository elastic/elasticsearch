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
     * Whether a ticket waiter may enqueue while its lease still holds {@link #untracked()} bytes.
     */
    enum UntrackedWait {
        /** Default. Waiting while this lease holds force-added bytes is the hang class. */
        FORBID,
        /**
         * Named exception for coalesced per-GET tickets. Waiting is allowed while this lease still
         * holds force-added bytes. Production coalesced admission uses {@link #FORBID}; this value
         * is the test-only unit-ticket path.
         */
        PER_GET
    }

    /**
     * FIFO ticket for {@code bytes}. Completes with a {@link Hold} on grant, or with failure on
     * cancel. Uncontended grants complete on the caller; contended grants are forked onto
     * {@code executor}. {@code lease} is required when the unit needs the overshoot slot;
     * {@code cancelSignal} is sampled at enqueue and at grant.
     * <p>
     * The caller must hold no force-added (untracked) bytes on {@code lease} while waiting,
     * unless {@code untrackedWait} names a listed exception. Rescue over-cap grants do not go
     * through this method.
     */
    default SubscribableListener<Hold> admitAsync(long bytes, RowGroupIo lease, BooleanSupplier cancelSignal, Executor executor) {
        return admitAsync(bytes, lease, cancelSignal, executor, UntrackedWait.FORBID);
    }

    SubscribableListener<Hold> admitAsync(
        long bytes,
        RowGroupIo lease,
        BooleanSupplier cancelSignal,
        Executor executor,
        UntrackedWait untrackedWait
    );

    /**
     * Unconditional charge for buffers that must exist (UNGATED alloc, sliding window) and for
     * decode-working-set shortfalls that exceed the footer estimate already in a hold. Never
     * waits. Pair shortfalls with {@link #release} when the working set drops. Counted in
     * {@link #untracked()} and attributed to {@code lease} when non-null, otherwise
     * {@link StorageIoAffinity#current()}.
     */
    default void add(long bytes) {
        add(bytes, null);
    }

    void add(long bytes, @Nullable RowGroupIo lease);

    /** Drops {@code bytes} of charge and grants the next waiter when there is room. */
    default void release(long bytes) {
        release(bytes, null);
    }

    void release(long bytes, @Nullable RowGroupIo lease);

    /**
     * Drops {@code lease} as the node-wide overshoot owner if it currently holds that slot,
     * unpins it, and re-runs the grant loop even when {@code lease} is not the owner. Does not
     * release bytes. Call this when the lease is done or cancelled; {@link Hold#close()} does
     * not, because sliding-window buffers may still sit in {@link #used()}.
     */
    void clearOwner(RowGroupIo lease);

    @Nullable
    RowGroupIo overshootOwner();

    long used();

    long limit();

    /**
     * Bytes charged by {@link #add} and not yet {@link #release}d. Ticketed {@link Hold}s are not
     * included. A waiting lease must not hold any of these.
     */
    long untracked();

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
     * Failure for a byte-ticket waiter whose lease has already {@link RowGroupIo#isFinished()
     * finished}. Same fate as {@link #cancelled()}: the waiter cannot pin, so it must not sit
     * on the FIFO head.
     */
    static EsRejectedExecutionException finished() {
        return new EsRejectedExecutionException("Lease finished while waiting for I/O bytes");
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
         * Unconditional extra charge. Credits {@link #remaining()} and never waits. Used to
         * grow a group reservation at a phase switch; the caller must not exceed the bytes
         * it is about to release. Callers serialize {@code grow} with {@link #close()}.
         */
        void grow(long bytes);

        /**
         * Releases leftover estimate. Does not {@link NodeByteBudget#clearOwner}: overshoot
         * occupancy can outlive this hold while force-added buffers remain.
         */
        @Override
        void close();
    }
}
