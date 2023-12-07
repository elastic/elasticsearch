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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Block build of BooleanBlocks.
 * This class is generated. Do not edit it.
 */
final class BooleanBlockBuilder extends AbstractBlockBuilder implements BooleanBlock.Builder {

    private interface BooleanValues extends Releasable {
        void setValue(int index, boolean value);
    }

    private static class PrimitiveBooleanValues implements BooleanValues {
        boolean[] array;

        PrimitiveBooleanValues(boolean[] array) {
            this.array = array;
        }

        @Override
        public void setValue(int index, boolean value) {
            array[index] = value;
        }

        @Override
        public void close() {

        }
    }

    private static class ArrayBooleanValues implements BooleanValues {
        final BitArray array;

        ArrayBooleanValues(BigArrays bigArrays, long size) {
            this.array = new BitArray(size, bigArrays);
        }

        @Override
        public void setValue(int index, boolean value) {
            if (value) {
                array.set(index);
            }
        }

        void grow(int minSize) {

        }

        @Override
        public void close() {
            array.close();
        }

        long ramBytesUsed() {
            return array.ramBytesUsed();
        }
    }

    private BooleanValues values;

    BooleanBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        long initialBytes = estimateBytesForBooleanArray(initialSize);
        if (initialBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            adjustBreaker(initialBytes);
            values = new PrimitiveBooleanValues(new boolean[initialSize]);
        } else {
            values = new ArrayBooleanValues(blockFactory.bigArrays(), initialSize);
        }
    }

    @Override
    public BooleanBlockBuilder appendBoolean(boolean value) {
        ensureCapacity();
        values.setValue(valueCount, value);
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    public BooleanBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public BooleanBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public BooleanBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public BooleanBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((BooleanBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public BooleanBlockBuilder appendAllValuesToCurrentPosition(BooleanBlock block) {
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
        final BooleanVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendBoolean(vector.getBoolean(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendBoolean(block.getBoolean(i++));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public BooleanBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((BooleanBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public BooleanBlockBuilder copyFrom(BooleanBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        BooleanVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(BooleanBlock block, int beginInclusive, int endExclusive) {
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
                appendBoolean(block.getBoolean(i++));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(BooleanVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendBoolean(vector.getBoolean(p));
        }
    }

    @Override
    public BooleanBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private BooleanBlock buildFromBigArrays(ArrayBooleanValues values) {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final BooleanBlock theBlock;
        if (isDense() && singleValued()) {
            theBlock = new BooleanBigArrayVector(values.array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new BooleanBigArrayBlock(values.array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
    public BooleanBlock build() {
        try {
            finish();
            final BooleanBlock theBlock;
            if (values instanceof ArrayBooleanValues arrayValues) {
                theBlock = buildFromBigArrays(arrayValues);
            } else {
                boolean[] array = ((PrimitiveBooleanValues) values).array;
                if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                    theBlock = blockFactory.newConstantBooleanBlockWith(array[0], 1, estimatedBytes);
                } else {
                    int currentLength = array.length;
                    // TODO: should be ANDed instead?
                    if (currentLength - valueCount > 1024 || valueCount < (currentLength / 2)) {
                        adjustBreaker(estimateBytesForBooleanArray(valueCount));
                        array = Arrays.copyOf(array, valueCount);
                        adjustBreaker(estimateBytesForBooleanArray(currentLength));
                    }
                    if (isDense() && singleValued()) {
                        theBlock = blockFactory.newBooleanArrayVector(array, positionCount, estimatedBytes).asBlock();
                    } else {
                        theBlock = blockFactory.newBooleanArrayBlock(
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
        if (values instanceof ArrayBooleanValues array) {
            array.grow(valueCount + 1);
            return;
        }
        final boolean[] array = ((PrimitiveBooleanValues) values).array;
        final int currentLength = array.length;
        if (valueCount < currentLength) {
            return;
        }
        final int newSize = currentLength + (currentLength >> 1);  // trivially, grows array by 50%
        final long newEstimatedBytes = estimateBytesForBooleanArray(newSize);
        if (newEstimatedBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            // extend the primitive array
            adjustBreaker(newEstimatedBytes);
            values = new PrimitiveBooleanValues(Arrays.copyOf(array, newSize));
            adjustBreaker(-estimateBytesForBooleanArray(currentLength));
            return;
        } else {
            // switch to a big array
            values = new ArrayBooleanValues(blockFactory.bigArrays(), valueCount + 1);
            for (int i = 0; i < currentLength; i++) {
                // TODO: bulk copy
                values.setValue(i, array[i]);
            }
            adjustBreaker(-estimateBytesForBooleanArray(currentLength));
        }
    }

    static long estimateBytesForBooleanArray(long arraySize) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + arraySize * (long) Byte.BYTES;
    }

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
