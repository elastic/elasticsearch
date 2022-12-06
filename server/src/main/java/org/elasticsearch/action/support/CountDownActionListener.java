/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.CountDown;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Wraps another listener and adds a counter -- each invocation of this listener will decrement the counter, and when the counter has been
 * exhausted the final invocation of this listener will delegate to the wrapped listener. Similar to {@link GroupedActionListener}, but for
 * the cases where tracking individual results is not useful.
 */
public final class CountDownActionListener extends ActionListener.Delegating<Void, Void> {

    private final CountDown countDown;
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
        countDown = new CountDown(groupSize);
    }

    @Override
    public void onResponse(Void element) {
        if (countDown.countDown()) {
            if (failure.get() != null) {
                super.onFailure(failure.get());
            } else {
                delegate.onResponse(element);
            }
        }
    }

    @Override
    public void onFailure(Exception e) {
        if (failure.compareAndSet(null, e) == false) {
            failure.accumulateAndGet(e, (current, update) -> {
                // we have to avoid self-suppression!
                if (update != current) {
                    current.addSuppressed(update);
                }
                return current;
            });
        }
        if (countDown.countDown()) {
            super.onFailure(failure.get());
        }
    }

}
