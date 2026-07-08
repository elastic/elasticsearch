/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * Releasable {@link RefCounted} base for {@link Block}s and {@link Vector}s. Calls to
 * {@link #decRef()} and {@link #close()} are equivalent.
 * <p>
 * Reference counting is thread safe (backed by a CAS loop) because a block's underlying data can
 * be shared -- via {@code incRef}, not a copy -- between sibling pages, e.g. a degenerate,
 * full-range {@link Block#slice}/{@link Block#filter}/{@link Block#keepMask} returns the same
 * instance rather than allocating one. Those sibling pages can end up released concurrently by
 * different threads once dispatched to background workers (e.g. by
 * {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}), so the reference count
 * must tolerate concurrent {@link #decRef()}/{@link #incRef()} calls on the same instance.
 * <p>
 * Uses a {@link VarHandle} over a plain {@code int} field (mirroring
 * {@link org.elasticsearch.core.AbstractRefCounted}) rather than an {@link
 * java.util.concurrent.atomic.AtomicInteger} field, so as not to inflate every block/vector's
 * shallow {@code ramBytesUsed()} with an extra heap-allocated object.
 */
public abstract class AbstractBlockRefCounted implements RefCounted, Releasable {

    private static final VarHandle VH_REFERENCES_FIELD;

    static {
        try {
            VH_REFERENCES_FIELD = MethodHandles.lookup()
                .in(AbstractBlockRefCounted.class)
                .findVarHandle(AbstractBlockRefCounted.class, "references", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // updated via VH_REFERENCES_FIELD (and _only_ via VH_REFERENCES_FIELD)
    private volatile int references = 1;
    private Releasable onClose;

    @Override
    public final void incRef() {
        if (tryIncRef() == false) {
            throw new IllegalStateException("can't increase refCount on already released object [" + this + "]");
        }
    }

    @Override
    public final boolean tryIncRef() {
        int current;
        do {
            current = references;
            if (current <= 0) {
                return false;
            }
        } while (VH_REFERENCES_FIELD.weakCompareAndSet(this, current, current + 1) == false);
        return true;
    }

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

    @Override
    public final boolean decRef() {
        int current;
        do {
            current = references;
            if (current <= 0) {
                throw new IllegalStateException("can't release already released object [" + this + "]");
            }
        } while (VH_REFERENCES_FIELD.weakCompareAndSet(this, current, current - 1) == false);

        if (current == 1) {
            closeInternal();
            Releasables.closeExpectNoException(onClose);
            return true;
        }
        return false;
    }

    @Override
    public final boolean hasReferences() {
        return references >= 1;
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
