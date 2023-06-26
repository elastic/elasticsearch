/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * An action listener that delegates its results to another listener once
 * it has received N results (either successes or failures). This allows synchronous
 * tasks to be forked off in a loop with the same listener and respond to a
 * higher level listener once all tasks responded.
 */
public final class GroupedActionListener<T> extends DelegatingActionListener<T, Collection<T>> {
    private final CountDown countDown;
    private final AtomicInteger pos = new AtomicInteger();
    private final AtomicArray<T> results;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    /**
     * Creates a new listener
     * @param groupSize the group size
     * @param delegate the delegate listener
     */
    public GroupedActionListener(int groupSize, ActionListener<Collection<T>> delegate) {
        super(delegate);
        if (groupSize <= 0) {
            assert false : "illegal group size [" + groupSize + "]";
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        }
        results = new AtomicArray<>(groupSize);
        countDown = new CountDown(groupSize);
    }

    @Override
    public void onResponse(T element) {
        results.setOnce(pos.incrementAndGet() - 1, element);
        if (countDown.countDown()) {
            if (failure.get() != null) {
                super.onFailure(failure.get());
            } else {
                List<T> collect = this.results.asList();
                delegate.onResponse(Collections.unmodifiableList(collect));
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        final var firstException = failure.compareAndExchange(null, e);
        if (firstException != null && firstException != e) {
            firstException.addSuppressed(e);
        }
        if (countDown.countDown()) {
            super.onFailure(failure.get());
        }
    }
}
