/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * A basic RefCounted implementation that is initialized with a
 * ref count of 1 and calls {@link #closeInternal()} once it reaches
 * a 0 ref count
 */
public abstract class AbstractRefCounted implements RefCounted {
    private final AtomicInteger refCount = new AtomicInteger(1);
    private final String name;

    public AbstractRefCounted(String name) {
        this.name = name;
    }

    @Override
    public final void incRef() {
        if (tryIncRef() == false) {
            alreadyClosed();
        }
    }

    @Override
    public final boolean tryIncRef() {
        do {
            int i = refCount.get();
            if (i > 0) {
                if (refCount.compareAndSet(i, i + 1)) {
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

    @Override
    public final boolean decRef() {
        int i = refCount.decrementAndGet();
        assert i >= 0;
        if (i == 0) {
            closeInternal();
            return true;
        }
        return false;
    }

    protected void alreadyClosed() {
        throw new IllegalStateException(name + " is already closed can't increment refCount current count [" + refCount.get() + "]");
    }

    /**
     * Returns the current reference count.
     */
    public int refCount() {
        return this.refCount.get();
    }


    /** gets the name of this instance */
    public String getName() {
        return name;
    }

    protected abstract void closeInternal();
}
