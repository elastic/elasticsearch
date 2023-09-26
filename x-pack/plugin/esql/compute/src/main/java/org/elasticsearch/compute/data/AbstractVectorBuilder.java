/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

abstract class AbstractVectorBuilder {
    protected int valueCount;

    protected final BlockFactory blockFactory;

    /** The number of bytes currently estimated with the breaker. */
    protected long estimatedBytes;

    protected AbstractVectorBuilder(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    /** The length of the internal values array. */
    protected abstract int valuesLength();

    protected abstract void growValuesArray(int newSize);

    /** The number of bytes used to represent each value element. */
    protected abstract int elementSize();

    protected final void ensureCapacity() {
        int valuesLength = valuesLength();
        if (valueCount < valuesLength) {
            return;
        }
        int newSize = calculateNewArraySize(valuesLength);
        adjustBreaker((long) (newSize - valuesLength) * elementSize());
        growValuesArray(newSize);
    }

    static int calculateNewArraySize(int currentSize) {
        // trivially, grows array by 50%
        return currentSize + (currentSize >> 1);
    }

    protected void adjustBreaker(long deltaBytes) {
        blockFactory.adjustBreaker(deltaBytes, false);
        estimatedBytes += deltaBytes;
    }
}
