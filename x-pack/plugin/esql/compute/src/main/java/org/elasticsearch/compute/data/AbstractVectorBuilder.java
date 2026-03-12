/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

public abstract class AbstractVectorBuilder implements Vector.Builder {
    protected int valueCount;

    /**
     * Has this builder been closed already?
     */
    private boolean closed = false;

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
        blockFactory.adjustBreaker(deltaBytes);
        estimatedBytes += deltaBytes;
    }

    /**
     * Called during implementations of {@link Block.Builder#build} as a first step
     * to check if the block is still open and to finish the last position.
     */
    protected final void finish() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
    }

    @Override
    public long estimatedBytes() {
        return estimatedBytes;
    }

    /**
     * Called during implementations of {@link Block.Builder#build} as a last step
     * to mark the Builder as closed and make sure that further closes don't double
     * free memory.
     */
    protected final void built() {
        closed = true;
        estimatedBytes = 0;
    }

    @Override
    public final void close() {
        if (closed == false) {
            closed = true;
            adjustBreaker(-estimatedBytes);
            extraClose();
        }
    }

    /**
     * Called when first {@link #close() closed}.
     */
    protected void extraClose() {}

    public boolean isReleased() {
        return closed;
    }
}
