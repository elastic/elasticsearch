/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Objects;

/**
 * {@link Releasable} {@link AbstractRefCounted} base for {@link Block}s and {@link Vector}s. Calls to
 * {@link #decRef()} and {@link #close()} are equivalent.
 * <p>
 * {@link AbstractRefCounted}'s reference count is already thread safe. That matters here because
 * a block's underlying data can be shared -- via {@code incRef} -- between sibling pages. These
 * sibling pages can be released concurrently by different threads once dispatched to background
 * workers (e.g. by {@link org.elasticsearch.compute.operator.topn.ParallelTopNOperator}).
 */
public abstract class AbstractBlockRefCounter extends AbstractRefCounted implements Releasable {
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

    @Override
    protected final void closeInternal() {
        closeBlock();
        Releasables.closeExpectNoException(onClose);
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
}
