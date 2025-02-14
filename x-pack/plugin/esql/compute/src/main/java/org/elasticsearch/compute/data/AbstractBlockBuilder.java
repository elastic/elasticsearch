/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.ArrayUtil;

import java.util.BitSet;
import java.util.stream.IntStream;

public abstract class AbstractBlockBuilder implements Block.Builder {

    protected final BlockFactory blockFactory;

    protected int[] firstValueIndexes; // lazily initialized, if multi-values

    protected BitSet nullsMask; // lazily initialized, if sparse

    protected int valueCount;

    protected int positionCount;

    protected boolean positionEntryIsOpen;

    protected boolean hasNonNullValue;
    protected boolean hasMultiValues;

    protected Block.MvOrdering mvOrdering = Block.MvOrdering.UNORDERED;

    /** The number of bytes currently estimated with the breaker. */
    protected long estimatedBytes;

    private boolean closed = false;

    protected AbstractBlockBuilder(BlockFactory blockFactory) {
        this.blockFactory = blockFactory;
    }

    @Override
    public AbstractBlockBuilder appendNull() {
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        ensureCapacity();
        if (nullsMask == null) {
            nullsMask = new BitSet();
        }
        nullsMask.set(positionCount);
        if (firstValueIndexes != null) {
            setFirstValue(positionCount, valueCount);
        }
        positionCount++;
        writeNullValue();
        valueCount++;
        return this;
    }

    protected void writeNullValue() {} // default is a no-op for array backed builders - since they have default value.

    /** The length of the internal values array. */
    protected abstract int valuesLength();

    @Override
    public AbstractBlockBuilder beginPositionEntry() {
        if (firstValueIndexes == null) {
            firstValueIndexes = new int[positionCount + 1];
            IntStream.range(0, positionCount).forEach(i -> firstValueIndexes[i] = i);
        }
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        positionEntryIsOpen = true;
        setFirstValue(positionCount, valueCount);
        return this;
    }

    public AbstractBlockBuilder endPositionEntry() {
        assert valueCount > firstValueIndexes[positionCount] : "use appendNull to build an empty position";
        positionCount++;
        positionEntryIsOpen = false;
        if (hasMultiValues == false && valueCount != positionCount) {
            hasMultiValues = true;
        }
        return this;
    }

    protected final boolean isDense() {
        return nullsMask == null;
    }

    protected final boolean singleValued() {
        return hasMultiValues == false;
    }

    protected final void updatePosition() {
        if (positionEntryIsOpen == false) {
            if (firstValueIndexes != null) {
                setFirstValue(positionCount, valueCount - 1);
            }
            positionCount++;
        }
    }

    /**
     * Called during implementations of {@link Block.Builder#build} as a first step
     * to check if the block is still open and to finish the last position.
     */
    protected final void finish() {
        if (closed) {
            throw new IllegalStateException("already closed");
        }
        if (positionEntryIsOpen) {
            endPositionEntry();
        }
        if (firstValueIndexes != null) {
            setFirstValue(positionCount, valueCount);
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

    protected abstract void growValuesArray(int newSize);

    /** The number of bytes used to represent each value element. */
    protected abstract int elementSize();

    protected final void ensureCapacity() {
        int valuesLength = valuesLength();
        if (valueCount < valuesLength) {
            return;
        }
        int newSize = ArrayUtil.oversize(valueCount, elementSize());
        adjustBreaker(newSize * elementSize());
        growValuesArray(newSize);
        adjustBreaker(-valuesLength * elementSize());
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

    protected void adjustBreaker(long deltaBytes) {
        blockFactory.adjustBreaker(deltaBytes);
        estimatedBytes += deltaBytes;
        assert estimatedBytes >= 0;
    }

    private void setFirstValue(int position, int value) {
        if (position >= firstValueIndexes.length) {
            final int currentSize = firstValueIndexes.length;
            // We grow the `firstValueIndexes` at the same rate as the `values` array, but independently.
            final int newLength = ArrayUtil.oversize(position + 1, Integer.BYTES);
            adjustBreaker((long) newLength * Integer.BYTES);
            firstValueIndexes = ArrayUtil.growExact(firstValueIndexes, newLength);
            adjustBreaker(-(long) currentSize * Integer.BYTES);
        }
        firstValueIndexes[position] = value;
    }

    public boolean isReleased() {
        return closed;
    }
}
