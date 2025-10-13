/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.util.concurrent;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Objects;

/**
 * Runnable that prevents running its delegate more than once.
 */
public class RunOnce implements Runnable {

    private static final VarHandle VH_DELEGATE_FIELD;

    static {
        try {
            VH_DELEGATE_FIELD = MethodHandles.lookup().in(RunOnce.class).findVarHandle(RunOnce.class, "delegate", Runnable.class);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("FieldMayBeFinal") // updated via VH_DELEGATE_FIELD (and _only_ via VH_DELEGATE_FIELD)
    private volatile Runnable delegate;

    public RunOnce(final Runnable delegate) {
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public void run() {
        var acquired = (Runnable) VH_DELEGATE_FIELD.compareAndExchange(this, delegate, null);
        if (acquired != null) {
            acquired.run();
        }
    }

    /**
     * {@code true} if the {@link RunOnce} has been executed once.
     */
    public boolean hasRun() {
        return delegate == null;
    }

    @Override
    public String toString() {
        return "RunOnce[" + delegate + "]";
    }
}
