/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nio;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

final class RoundRobinSupplier<S> implements Supplier<S> {

    private final AtomicBoolean selectorsSet = new AtomicBoolean(false);
    private volatile S[] selectors;
    private AtomicInteger counter = new AtomicInteger(0);

    RoundRobinSupplier() {
        this.selectors = null;
    }

    RoundRobinSupplier(S[] selectors) {
        this.selectors = selectors;
        this.selectorsSet.set(true);
    }

    @Override
    public S get() {
        S[] selectors = this.selectors;
        return selectors[counter.getAndIncrement() % selectors.length];
    }

    void setSelectors(S[] selectors) {
        if (selectorsSet.compareAndSet(false, true)) {
            this.selectors = selectors;
        } else {
            throw new AssertionError("Selectors already set. Should only be set once.");
        }
    }

    int count() {
        return selectors.length;
    }
}
