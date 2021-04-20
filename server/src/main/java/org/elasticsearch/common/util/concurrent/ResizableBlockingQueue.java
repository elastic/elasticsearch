/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import java.util.concurrent.BlockingQueue;

/**
 * Extends the {@code SizeBlockingQueue} to add the {@code adjustCapacity} method, which will adjust
 * the capacity by a certain amount towards a maximum or minimum.
 */
final class ResizableBlockingQueue<E> extends SizeBlockingQueue<E> {

    private volatile int capacity;

    ResizableBlockingQueue(BlockingQueue<E> queue, int initialCapacity) {
        super(queue, initialCapacity);
        this.capacity = initialCapacity;
    }

    @Override
    public int capacity() {
        return this.capacity;
    }

    @Override
    public int remainingCapacity() {
        return Math.max(0, this.capacity());
    }

    /** Resize the limit for the queue, returning the new size limit */
    public synchronized int adjustCapacity(int optimalCapacity, int adjustmentAmount, int minCapacity, int maxCapacity) {
        assert adjustmentAmount > 0 : "adjustment amount should be a positive value";
        assert optimalCapacity >= 0 : "desired capacity cannot be negative";
        assert minCapacity >= 0 : "cannot have min capacity smaller than 0";
        assert maxCapacity >= minCapacity : "cannot have max capacity smaller than min capacity";

        if (optimalCapacity == capacity) {
            // Yahtzee!
            return this.capacity;
        }

        if (optimalCapacity > capacity + adjustmentAmount) {
            // adjust up
            final int newCapacity = Math.min(maxCapacity, capacity + adjustmentAmount);
            this.capacity = newCapacity;
            return newCapacity;
        } else if (optimalCapacity < capacity - adjustmentAmount) {
            // adjust down
            final int newCapacity = Math.max(minCapacity, capacity - adjustmentAmount);
            this.capacity = newCapacity;
            return newCapacity;
        } else {
            return this.capacity;
        }
    }
}
