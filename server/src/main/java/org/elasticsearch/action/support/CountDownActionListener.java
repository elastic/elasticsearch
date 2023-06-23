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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps another listener and adds a counter -- each invocation of this listener will decrement the counter, and when the counter has been
 * exhausted the final invocation of this listener will delegate to the wrapped listener. Similar to {@link GroupedActionListener}, but for
 * the cases where tracking individual results is not useful.
 */
public final class CountDownActionListener extends DelegatingActionListener<Void, Void> {

    private final AtomicInteger countDown;
    private final AtomicReference<Exception> failure = new AtomicReference<>();

    /**
     * Creates a new listener
     * @param groupSize the group size
     * @param delegate the delegate listener
     */
    public CountDownActionListener(int groupSize, ActionListener<Void> delegate) {
        super(Objects.requireNonNull(delegate));
        if (groupSize <= 0) {
            assert false : "illegal group size [" + groupSize + "]";
            throw new IllegalArgumentException("groupSize must be greater than 0 but was " + groupSize);
        }
        countDown = new AtomicInteger(groupSize);
    }

    private boolean countDown() {
        final var result = countDown.getAndUpdate(current -> Math.max(0, current - 1));
        assert result > 0;
        return result == 1;
    }

    @Override
    public void onResponse(Void element) {
        if (countDown()) {
            if (failure.get() != null) {
                super.onFailure(failure.get());
            } else {
                delegate.onResponse(element);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        final var firstException = failure.compareAndExchange(null, e);
        if (firstException != null && firstException != e) {
            firstException.addSuppressed(e);
        }
        if (countDown()) {
            super.onFailure(failure.get());
        }
    }

}
