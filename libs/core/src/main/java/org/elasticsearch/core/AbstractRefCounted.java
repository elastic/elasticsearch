/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

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
                    touch();
                    return true;
                }
            } else {
                return false;
            }
        } while (true);
    }

    @Override
    public final boolean decRef() {
        touch();
        int i = refCount.decrementAndGet();
        assert i >= 0;
        if (i == 0) {
            try {
                closeInternal();
            } catch (Exception e) {
                assert false : e;
                throw e;
            }
            return true;
        }
        return false;
    }

    /**
     * Called whenever the ref count is incremented or decremented. Can be implemented by implementations to a record of access to the
     * instance for debugging purposes.
     */
    protected void touch() {
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

    /**
     * Method that is invoked once the reference count reaches zero.
     * Implementations of this method must handle all exceptions and may not throw any exceptions.
     */
    protected abstract void closeInternal();
}
