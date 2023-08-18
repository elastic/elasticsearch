/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A simple thread-safe count-down class that does not block, unlike a {@link CountDownLatch}. This class is useful if an action must wait
 * for N concurrent tasks to succeed, or some other task to fail, in order to proceed. When called enough times, exactly one invocation of
 * {@link #countDown()} or {@link #fastForward()} will return {@code true}.
 */
public final class CountDown {

    private final AtomicInteger countDown;

    public CountDown(int count) {
        if (count <= 0) {
            final String message = "count must be greater than 0 but was: " + count;
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        this.countDown = new AtomicInteger(count);
    }

    private static int assertValidCount(int count) {
        assert count >= 0 : count;
        return count;
    }

    /**
     * Decrements the count and returns {@code true} if and only if the count reached zero with this call.
     */
    public boolean countDown() {
        return countDown.getAndUpdate(current -> assertValidCount(current) == 0 ? 0 : current - 1) == 1;
    }

    /**
     * Fast-forwards the count to zero and returns {@code true} if and only if the count reached zero with this call.
     */
    public boolean fastForward() {
        return assertValidCount(countDown.getAndSet(0)) > 0;
    }

    /**
     * Returns {@code true} if and only if the count has reached zero.
     */
    public boolean isCountedDown() {
        return assertValidCount(countDown.get()) == 0;
    }
}
