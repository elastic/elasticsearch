/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;

/**
 * Releasable, non-threadsafe version of {@link org.elasticsearch.core.AbstractRefCounted}.
 * Calls to {@link AbstractNonThreadSafeRefCounted#decRef()} and {@link AbstractNonThreadSafeRefCounted#close()} are equivalent.
 */
abstract class AbstractNonThreadSafeRefCounted implements RefCounted, Releasable {
    private int references = 1;

    @Override
    public final void incRef() {
        if (hasReferences() == false) {
            throw new IllegalStateException("can't increase refCount on already released object [" + this + "]");
        }
        references++;
    }

    @Override
    public final boolean tryIncRef() {
        if (hasReferences() == false) {
            return false;
        }
        references++;
        return true;
    }

    @Override
    public final boolean decRef() {
        if (hasReferences() == false) {
            throw new IllegalStateException("can't release already released object [" + this + "]");
        }

        references--;

        if (references <= 0) {
            closeInternal();
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
