/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * {@link Releasable} {@link RefCounted} base for {@link Block}s and {@link Vector}s. Calls to
 * {@link #decRef()} and {@link #close()} are equivalent.
 * <p>
 * Most {@link Block}s and {@link Vector}s are only ever touched by the single thread that created
 * them, so this starts out backed by a plain, non-atomic {@code int} -- cheaper than a CAS loop
 * per {@code incRef}/{@code decRef}. A block's underlying data can, however, be shared -- via
 * {@code incRef} -- between sibling pages, and those sibling pages can be released concurrently
 * by different threads once dispatched to background workers (e.g. by
 * {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}). Callers must invoke
 * {@link #makeRefCountsThreadSafe()} before doing that, e.g. from
 * {@link Block#allowPassingToDifferentDriver()}, to switch to a thread-safe implementation that
 * uses a CAS loop over a separate embedded {@code volatile int} field -- the same technique used
 * by {@link AbstractRefCounted}, without any separate heap allocation.
 */
public abstract class AbstractBlockRefCounted implements RefCounted, Releasable {

    private static final VarHandle VH_ATOMIC_REFCOUNT_FIELD;

    static {
        try {
            VH_ATOMIC_REFCOUNT_FIELD = MethodHandles.lookup()
                .in(AbstractBlockRefCounted.class)
                .findVarHandle(AbstractBlockRefCounted.class, "atomicRefCount", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /** Plain (non-atomic) ref count used while single-threaded, before promotion. */
    private int refCount = 1;
    /**
     * Atomic ref count used after promotion. Initialized from {@link #refCount} by
     * {@link #makeRefCountsThreadSafe()}; mutated only via {@link #VH_ATOMIC_REFCOUNT_FIELD}.
     */
    @SuppressWarnings("FieldMayBeFinal") // updated via VH_ATOMIC_REFCOUNT_FIELD after promotion
    private volatile int atomicRefCount;
    /**
     * When {@code true}, all ref-count mutations use the CAS path over {@link #atomicRefCount}.
     * Set once by the owning thread before safe publication; never cleared.
     */
    private boolean threadSafe;
    private Releasable onClose;

    /**
     * Attaches a {@link Releasable} that is invoked exactly once when this object's reference count reaches zero,
     * immediately after {@link #closeInternal()} completes. May be called at most once; throws
     * {@link IllegalStateException} if called after release or a second time.
     */
    public final void attachReleasable(Releasable releasable) {
        Objects.requireNonNull(releasable, "releasable must not be null");
        if (hasReferences() == false) {
            throw new IllegalStateException("can't attach releasable to already released object [" + this + "]");
        }
        if (this.onClose != null) {
            throw new IllegalStateException("onClose already attached to [" + this + "]");
        }
        this.onClose = releasable;
    }

    /**
     * Switches this object's reference counting to a thread-safe mode that uses a CAS loop over an
     * embedded {@code volatile int}. Must be called by the single thread that currently owns this
     * object, before any reference to it can be used concurrently from more than one thread.
     * Idempotent: a no-op if already thread-safe.
     */
    public final void makeRefCountsThreadSafe() {
        if (threadSafe) {
            return;
        }
        atomicRefCount = refCount;
        threadSafe = true;
    }

    @Override
    public final void incRef() {
        if (tryIncRef() == false) {
            throw new IllegalStateException(AbstractRefCounted.ALREADY_CLOSED_MESSAGE);
        }
    }

    @Override
    public final boolean tryIncRef() {
        if (threadSafe) {
            do {
                int i = atomicRefCount;
                if (i > 0) {
                    if (VH_ATOMIC_REFCOUNT_FIELD.weakCompareAndSet(this, i, i + 1)) {
                        return true;
                    }
                } else {
                    return false;
                }
            } while (true);
        } else {
            if (refCount <= 0) {
                return false;
            }
            refCount++;
            return true;
        }
    }

    @Override
    public final boolean decRef() {
        boolean closed;
        if (threadSafe) {
            int i = (int) VH_ATOMIC_REFCOUNT_FIELD.getAndAdd(this, -1);
            if (i <= 0) {
                throw new IllegalStateException("can't release already released object [" + this + "]");
            }
            closed = (i == 1);
        } else {
            if (refCount <= 0) {
                throw new IllegalStateException("can't release already released object [" + this + "]");
            }
            closed = --refCount == 0;
        }
        if (closed) {
            closeInternal();
            Releasables.closeExpectNoException(onClose);
        }
        return closed;
    }

    @Override
    public final boolean hasReferences() {
        if (threadSafe) {
            return atomicRefCount > 0;
        }
        return refCount > 0;
    }

    @Override
    public final void close() {
        decRef();
    }

    public final boolean isReleased() {
        return hasReferences() == false;
    }

    /**
     * This is called when the number of references reaches zero.
     * This is where resources should be released (adjusting circuit breakers if needed).
     */
    protected abstract void closeInternal();
}
