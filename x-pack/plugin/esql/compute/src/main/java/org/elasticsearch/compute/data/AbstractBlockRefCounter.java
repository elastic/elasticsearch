/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * {@link Releasable} {@link RefCounted} base for {@link Block}s and {@link Vector}s. Calls to
 * {@link #decRef()} and {@link #close()} are equivalent.
 * <p>
 * Most {@link Block}s and {@link Vector}s are only ever touched by the single thread that created
 * them, so this starts out backed by a plain, non-atomic {@link NonAtomicRefCount} -- cheaper than
 * a CAS loop per {@code incRef}/{@code decRef}. A block's underlying data can, however, be shared --
 * via {@code incRef} -- between sibling pages, and those sibling pages can be released
 * concurrently by different threads once dispatched to background workers (e.g. by
 * {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}). Callers must invoke
 * {@link #makeRefCountsAtomic()} before doing that, e.g. from {@link Block#allowPassingToDifferentDriver()},
 * to switch to a thread-safe reference count backed by {@link AbstractRefCounted}'s CAS loop.
 */
public abstract class AbstractBlockRefCounter implements RefCounted, Releasable {
    /**
     * Shallow size of the separate reference-count object every instance allocates -- a
     * {@link NonAtomicRefCount} by default, or its atomic equivalent (the same size) once promoted
     * via {@link #makeRefCountsAtomic()}. Subclasses' own {@code BASE_RAM_BYTES_USED} constants must
     * add this in, since it isn't part of the subclass's own shallow size but is nonetheless
     * retained memory.
     */
    public static final long REF_COUNT_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(NonAtomicRefCount.class);

    private RefCounted refCount = new NonAtomicRefCount();
    private Releasable onClose;

    /**
     * Attaches a {@link Releasable} that is invoked exactly once when this object's reference count reaches zero,
     * immediately after {@link #closeBlock()} completes. May be called at most once; throws
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
     * Upgrades this object's reference counting to a thread-safe implementation, preserving the
     * current reference count. Must be called by the single thread that currently owns this
     * object, before any reference to it can be used concurrently from more than one thread.
     * Idempotent: a no-op if already promoted.
     */
    public final void makeRefCountsAtomic() {
        if (refCount instanceof NonAtomicRefCount nonAtomic) {
            refCount = nonAtomic.toAtomic();
        }
    }

    @Override
    public final void incRef() {
        refCount.incRef();
    }

    @Override
    public final boolean tryIncRef() {
        return refCount.tryIncRef();
    }

    @Override
    public final boolean decRef() {
        if (refCount.decRef()) {
            closeBlock();
            Releasables.closeExpectNoException(onClose);
            return true;
        }
        return false;
    }

    @Override
    public final boolean hasReferences() {
        return refCount.hasReferences();
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
    protected abstract void closeBlock();

    /**
     * Plain, non-atomic reference count -- the default, cheap fast path for objects never shared
     * across threads. {@link #makeRefCountsAtomic()} replaces this with an {@link AtomicRefCount}
     * once this object may be accessed concurrently.
     */
    private static class NonAtomicRefCount implements RefCounted {
        private int count = 1;

        @Override
        public boolean tryIncRef() {
            if (count <= 0) {
                return false;
            }
            count++;
            return true;
        }

        @Override
        public void incRef() {
            if (tryIncRef() == false) {
                assert count == 0 : count;
                throw new IllegalStateException(AbstractRefCounted.ALREADY_CLOSED_MESSAGE);
            }
        }

        @Override
        public boolean decRef() {
            int before = count--;
            assert before > 0 : AbstractRefCounted.INVALID_DECREF_MESSAGE;
            return before == 1;
        }

        @Override
        public boolean hasReferences() {
            return count > 0;
        }

        RefCounted toAtomic() {
            AtomicRefCount atomic = new AtomicRefCount();
            for (int i = 1; i < count; i++) {
                atomic.incRef();
            }
            return atomic;
        }

        /**
         * Thread-safe reference count, backed by {@link AbstractRefCounted}'s CAS loop over a
         * volatile int. {@link #closeInternal()} is a no-op because {@link AbstractBlockRefCounter#decRef()}
         * already runs {@link AbstractBlockRefCounter#closeBlock()} and the attached releasable once the
         * delegate reports the count reached zero.
         */
        private static final class AtomicRefCount extends AbstractRefCounted {
            @Override
            protected void closeInternal() {}
        }
    }
}
