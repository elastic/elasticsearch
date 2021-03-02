/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

import org.elasticsearch.common.util.concurrent.ConcurrentCollections;

import java.util.Deque;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A {@link Recycler} implementation based on a concurrent {@link Deque}. This implementation is thread-safe.
 */
public class ConcurrentDequeRecycler<T> extends DequeRecycler<T> {

    // we maintain size separately because concurrent deque implementations typically have linear-time size() impls
    final AtomicInteger size;

    public ConcurrentDequeRecycler(C<T> c, int maxSize) {
        super(c, ConcurrentCollections.newDeque(), maxSize);
        this.size = new AtomicInteger();
    }

    @Override
    public V<T> obtain() {
        final V<T> v = super.obtain();
        if (v.isRecycled()) {
            size.decrementAndGet();
        }
        return v;
    }

    @Override
    protected boolean beforeRelease() {
        return size.incrementAndGet() <= maxSize;
    }

    @Override
    protected void afterRelease(boolean recycled) {
        if (recycled == false) {
            size.decrementAndGet();
        }
    }

}
