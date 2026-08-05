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

    /**
     * Cancels the current position entry, discarding all values appended since the last
     * {@link #beginPositionEntry()} call. After this call the builder is in the same state
     * as before {@code beginPositionEntry} was called: the caller must immediately either
     * start a new position entry or append a null or a scalar value for the current position.
     *
     * <p>Subclasses that maintain auxiliary storage beyond the base {@code valueCount} and
     * values array (e.g., {@code BytesRefBlockBuilder} with its {@code BytesRefArray}) must
     * override this to also roll back that auxiliary storage to the point recorded by
     * {@link #beginPositionEntry()}.
     */
    public AbstractBlockBuilder cancelPositionEntry() {
        assert positionEntryIsOpen : "cancelPositionEntry called without a matching beginPositionEntry";
        valueCount = firstValueIndexes[positionCount];
        positionEntryIsOpen = false;
        // If rolling back brings valueCount to zero there are no previously committed non-null values:
        // reset hasNonNullValue so build() does not take the constant-vector fast path while nullsMask is set.
        if (valueCount == 0) {
            hasNonNullValue = false;
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
     * Registers {@code numValuesAppended} new single-valued positions in bulk.
     * All values must already have been written to the values array and
     * {@code valueCount} must already reflect them.
     */
    protected final void updatePositions(int numValuesAppended) {
        if (positionEntryIsOpen) {
            return;
        }
        if (firstValueIndexes != null) {
            ensureFirstValueIndexesCapacity(positionCount + numValuesAppended);
            int firstValue = valueCount - numValuesAppended;
            for (int i = 0; i < numValuesAppended; i++) {
                firstValueIndexes[positionCount + i] = firstValue + i;
            }
        }
        positionCount += numValuesAppended;
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
        ensureCapacity(1);
    }

    /**
     * Ensures the values array has room for at least {@code additionalValueCount} more values.
     */
    protected final void ensureCapacity(int additionalValueCount) {
        int valuesLength = valuesLength();
        int requiredSize = valueCount + additionalValueCount;
        if (requiredSize <= valuesLength) {
            return;
        }
        int newSize = ArrayUtil.oversize(requiredSize, elementSize());
        adjustBreaker((long) newSize * elementSize());
        growValuesArray(newSize);
        adjustBreaker(-(long) valuesLength * elementSize());
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

    private void ensureFirstValueIndexesCapacity(int minSize) {
        if (minSize <= firstValueIndexes.length) {
            return;
        }
        final int currentSize = firstValueIndexes.length;
        final int newLength = ArrayUtil.oversize(minSize, Integer.BYTES);
        adjustBreaker((long) newLength * Integer.BYTES);
        firstValueIndexes = ArrayUtil.growExact(firstValueIndexes, newLength);
        adjustBreaker(-(long) currentSize * Integer.BYTES);
    }

    private void setFirstValue(int position, int value) {
        ensureFirstValueIndexesCapacity(position + 1);
        firstValueIndexes[position] = value;
    }

    public boolean isReleased() {
        return closed;
    }
}
