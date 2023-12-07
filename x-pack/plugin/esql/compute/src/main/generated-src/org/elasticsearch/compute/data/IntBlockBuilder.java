/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Block build of IntBlocks.
 * This class is generated. Do not edit it.
 */
final class IntBlockBuilder extends AbstractBlockBuilder implements IntBlock.Builder {

    private interface IntValues extends Releasable {
        void setValue(int index, int value);
    }

    private static class PrimitiveIntValues implements IntValues {
        int[] array;

        PrimitiveIntValues(int[] array) {
            this.array = array;
        }

        @Override
        public void setValue(int index, int value) {
            array[index] = value;
        }

        @Override
        public void close() {

        }
    }

    private static class ArrayIntValues implements IntValues {
        final BigArrays bigArrays;
        IntArray array;

        ArrayIntValues(BigArrays bigArrays, long size) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newIntArray(size, false);
        }

        @Override
        public void setValue(int index, int value) {
            array.set(index, value);
        }

        void grow(int minSize) {
            array = bigArrays.grow(array, minSize);
        }

        @Override
        public void close() {
            array.close();
        }

        long ramBytesUsed() {
            return array.ramBytesUsed();
        }
    }

    private IntValues values;

    IntBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        long initialBytes = estimateBytesForIntArray(initialSize);
        if (initialBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            adjustBreaker(initialBytes);
            values = new PrimitiveIntValues(new int[initialSize]);
        } else {
            values = new ArrayIntValues(blockFactory.bigArrays(), initialSize);
        }
    }

    @Override
    public IntBlockBuilder appendInt(int value) {
        ensureCapacity();
        values.setValue(valueCount, value);
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    public IntBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public IntBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public IntBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public IntBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((IntBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public IntBlockBuilder appendAllValuesToCurrentPosition(IntBlock block) {
        final int positionCount = block.getPositionCount();
        if (positionCount == 0) {
            return appendNull();
        }
        final int totalValueCount = block.getTotalValueCount();
        if (totalValueCount == 0) {
            return appendNull();
        }
        if (totalValueCount > 1) {
            beginPositionEntry();
        }
        final IntVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendInt(vector.getInt(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendInt(block.getInt(i++));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public IntBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((IntBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public IntBlockBuilder copyFrom(IntBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        IntVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(IntBlock block, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            if (block.isNull(p)) {
                appendNull();
                continue;
            }
            int count = block.getValueCount(p);
            if (count > 1) {
                beginPositionEntry();
            }
            int i = block.getFirstValueIndex(p);
            for (int v = 0; v < count; v++) {
                appendInt(block.getInt(i++));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(IntVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendInt(vector.getInt(p));
        }
    }

    @Override
    public IntBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private IntBlock buildFromBigArrays(ArrayIntValues values) {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final IntBlock theBlock;
        if (isDense() && singleValued()) {
            theBlock = new IntBigArrayVector(values.array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new IntBigArrayBlock(values.array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - values.ramBytesUsed(), false);
        return theBlock;
    }

    @Override
    public IntBlock build() {
        try {
            finish();
            final IntBlock theBlock;
            if (values instanceof ArrayIntValues arrayValues) {
                theBlock = buildFromBigArrays(arrayValues);
            } else {
                int[] array = ((PrimitiveIntValues) values).array;
                if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                    theBlock = blockFactory.newConstantIntBlockWith(array[0], 1, estimatedBytes);
                } else {
                    int currentLength = array.length;
                    // TODO: should be ANDed instead?
                    if (currentLength - valueCount > 1024 || valueCount < (currentLength / 2)) {
                        adjustBreaker(estimateBytesForIntArray(valueCount));
                        array = Arrays.copyOf(array, valueCount);
                        adjustBreaker(estimateBytesForIntArray(currentLength));
                    }
                    if (isDense() && singleValued()) {
                        theBlock = blockFactory.newIntArrayVector(array, positionCount, estimatedBytes).asBlock();
                    } else {
                        theBlock = blockFactory.newIntArrayBlock(
                            array,
                            positionCount,
                            firstValueIndexes,
                            nullsMask,
                            mvOrdering,
                            estimatedBytes
                        );
                    }
                }
            }
            values = null;
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }

    @Override
    protected void ensureCapacity() {
        if (values instanceof ArrayIntValues array) {
            array.grow(valueCount + 1);
            return;
        }
        final int[] array = ((PrimitiveIntValues) values).array;
        final int currentLength = array.length;
        if (valueCount < currentLength) {
            return;
        }
        final int newSize = currentLength + (currentLength >> 1);  // trivially, grows array by 50%
        final long newEstimatedBytes = estimateBytesForIntArray(newSize);
        if (newEstimatedBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            // extend the primitive array
            adjustBreaker(newEstimatedBytes);
            values = new PrimitiveIntValues(Arrays.copyOf(array, newSize));
            adjustBreaker(-estimateBytesForIntArray(currentLength));
            return;
        } else {
            // switch to a big array
            values = new ArrayIntValues(blockFactory.bigArrays(), valueCount + 1);
            for (int i = 0; i < currentLength; i++) {
                // TODO: bulk copy
                values.setValue(i, array[i]);
            }
            adjustBreaker(-estimateBytesForIntArray(currentLength));
        }
    }

    static long estimateBytesForIntArray(long arraySize) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + arraySize * (long) Integer.BYTES;
    }

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
