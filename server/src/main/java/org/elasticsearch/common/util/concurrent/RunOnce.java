/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Runnable that prevents running its delegate more than once.
 */
public class RunOnce implements Runnable {

    private final AtomicReference<Runnable> delegateRef;

    public RunOnce(final Runnable delegate) {
        delegateRef = new AtomicReference<>(Objects.requireNonNull(delegate));
    }

    @Override
    public void run() {
        var acquired = delegateRef.getAndSet(null);
        if (acquired != null) {
            acquired.run();
        }
    }

    /**
     * {@code true} if the {@link RunOnce} has been executed once.
     */
    public boolean hasRun() {
        return delegateRef.get() == null;
    }

    @Override
    public String toString() {
        return "RunOnce[" + delegateRef.get() + "]";
    }
}
