/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.util.concurrent;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runnable that can only be run one time.
 */
public class RunOnce implements Runnable {

    private final Runnable delegate;
    private final AtomicBoolean hasRun;

    public RunOnce(final Runnable delegate) {
        this.delegate = Objects.requireNonNull(delegate);
        this.hasRun = new AtomicBoolean(false);
    }

    @Override
    public void run() {
        if (hasRun.compareAndSet(false, true)) {
            delegate.run();
        }
    }

    /**
     * {@code true} if the {@link RunOnce} has been executed once.
     */
    public boolean hasRun() {
        return hasRun.get();
    }
}
