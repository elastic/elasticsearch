/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * A basic {@link RefCounted} implementation that is initialized with a ref count of 1 and calls {@link #closeInternal()} once it reaches
 * a 0 ref count.
 */
public abstract class AbstractRefCounted implements RefCounted {

    public static final String ALREADY_CLOSED_MESSAGE = "already closed, can't increment ref count";
    public static final String INVALID_DECREF_MESSAGE = "invalid decRef call: already closed";

    private static final VarHandle VH_REFCOUNT_FIELD;

    static {
        try {
            VH_REFCOUNT_FIELD = MethodHandles.lookup()
                .in(AbstractRefCounted.class)
                .findVarHandle(AbstractRefCounted.class, "refCount", int.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // updated via VH_REFCOUNT_FIELD (and _only_ via VH_REFCOUNT_FIELD)
    private volatile int refCount = 1;

    protected AbstractRefCounted() {}

    @Override
    public final void incRef() {
        if (tryIncRef() == false) {
            alreadyClosed();
        }
    }

    @Override
    public final void mustIncRef() {
        // making this implementation `final` (to be consistent with every other `RefCounted` method implementation)
        RefCounted.super.mustIncRef();
    }

    @Override
    public final boolean tryIncRef() {
        do {
            int i = refCount;
            if (i > 0) {
                if (VH_REFCOUNT_FIELD.weakCompareAndSet(this, i, i + 1)) {
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
        int i = (int) VH_REFCOUNT_FIELD.getAndAdd(this, -1);
        assert i > 0 : INVALID_DECREF_MESSAGE;
        if (i == 1) {
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

    @Override
    public final boolean hasReferences() {
        return refCount > 0;
    }

    /**
     * Called whenever the ref count is incremented or decremented. Can be overridden to record access to the instance for debugging
     * purposes.
     */
    protected void touch() {}

    protected void alreadyClosed() {
        final int currentRefCount = refCount;
        assert currentRefCount == 0 : currentRefCount;
        throw new IllegalStateException(ALREADY_CLOSED_MESSAGE);
    }

    /**
     * Returns the current reference count.
     */
    public final int refCount() {
        return refCount;
    }

    /**
     * Method that is invoked once the reference count reaches zero.
     * Implementations of this method must handle all exceptions and may not throw any exceptions.
     */
    protected abstract void closeInternal();

    /**
     * Construct an {@link AbstractRefCounted} which runs the given {@link Runnable} when all references are released.
     */
    public static AbstractRefCounted of(Runnable onClose) {
        Objects.requireNonNull(onClose);
        return new AbstractRefCounted() {
            @Override
            protected void closeInternal() {
                onClose.run();
            }

            @Override
            public String toString() {
                return "refCounted[" + onClose + "]";
            }
        };
    }

}
