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
import org.elasticsearch.common.util.DoubleArray;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;

/**
 * Block build of DoubleBlocks.
 * This class is generated. Do not edit it.
 */
final class DoubleBlockBuilder extends AbstractBlockBuilder implements DoubleBlock.Builder {

    private interface DoubleValues extends Releasable {
        void setValue(int index, double value);
    }

    private static class PrimitiveDoubleValues implements DoubleValues {
        double[] array;

        PrimitiveDoubleValues(double[] array) {
            this.array = array;
        }

        @Override
        public void setValue(int index, double value) {
            array[index] = value;
        }

        @Override
        public void close() {

        }
    }

    private static class ArrayDoubleValues implements DoubleValues {
        final BigArrays bigArrays;
        DoubleArray array;

        ArrayDoubleValues(BigArrays bigArrays, long size) {
            this.bigArrays = bigArrays;
            this.array = bigArrays.newDoubleArray(size, false);
        }

        @Override
        public void setValue(int index, double value) {
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

    private DoubleValues values;

    DoubleBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        long initialBytes = estimateBytesForDoubleArray(initialSize);
        if (initialBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            adjustBreaker(initialBytes);
            values = new PrimitiveDoubleValues(new double[initialSize]);
        } else {
            values = new ArrayDoubleValues(blockFactory.bigArrays(), initialSize);
        }
    }

    @Override
    public DoubleBlockBuilder appendDouble(double value) {
        ensureCapacity();
        values.setValue(valueCount, value);
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    public DoubleBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public DoubleBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public DoubleBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public DoubleBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((DoubleBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public DoubleBlockBuilder appendAllValuesToCurrentPosition(DoubleBlock block) {
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
        final DoubleVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendDouble(vector.getDouble(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendDouble(block.getDouble(i++));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public DoubleBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((DoubleBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public DoubleBlockBuilder copyFrom(DoubleBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        DoubleVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(DoubleBlock block, int beginInclusive, int endExclusive) {
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
                appendDouble(block.getDouble(i++));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(DoubleVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendDouble(vector.getDouble(p));
        }
    }

    @Override
    public DoubleBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private DoubleBlock buildFromBigArrays(ArrayDoubleValues values) {
        assert estimatedBytes == 0 || firstValueIndexes != null;
        final DoubleBlock theBlock;
        if (isDense() && singleValued()) {
            theBlock = new DoubleBigArrayVector(values.array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new DoubleBigArrayBlock(values.array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
    public DoubleBlock build() {
        try {
            finish();
            final DoubleBlock theBlock;
            if (values instanceof ArrayDoubleValues arrayValues) {
                theBlock = buildFromBigArrays(arrayValues);
            } else {
                double[] array = ((PrimitiveDoubleValues) values).array;
                if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                    theBlock = blockFactory.newConstantDoubleBlockWith(array[0], 1, estimatedBytes);
                } else {
                    int currentLength = array.length;
                    // TODO: should be ANDed instead?
                    if (currentLength - valueCount > 1024 || valueCount < (currentLength / 2)) {
                        adjustBreaker(estimateBytesForDoubleArray(valueCount));
                        array = Arrays.copyOf(array, valueCount);
                        adjustBreaker(estimateBytesForDoubleArray(currentLength));
                    }
                    if (isDense() && singleValued()) {
                        theBlock = blockFactory.newDoubleArrayVector(array, positionCount, estimatedBytes).asBlock();
                    } else {
                        theBlock = blockFactory.newDoubleArrayBlock(
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
        if (values instanceof ArrayDoubleValues array) {
            array.grow(valueCount + 1);
            return;
        }
        final double[] array = ((PrimitiveDoubleValues) values).array;
        final int currentLength = array.length;
        if (valueCount < currentLength) {
            return;
        }
        final int newSize = currentLength + (currentLength >> 1);  // trivially, grows array by 50%
        final long newEstimatedBytes = estimateBytesForDoubleArray(newSize);
        if (newEstimatedBytes <= blockFactory.maxPrimitiveArrayBytes()) {
            // extend the primitive array
            adjustBreaker(newEstimatedBytes);
            values = new PrimitiveDoubleValues(Arrays.copyOf(array, newSize));
            adjustBreaker(-estimateBytesForDoubleArray(currentLength));
            return;
        } else {
            // switch to a big array
            values = new ArrayDoubleValues(blockFactory.bigArrays(), valueCount + 1);
            for (int i = 0; i < currentLength; i++) {
                // TODO: bulk copy
                values.setValue(i, array[i]);
            }
            adjustBreaker(-estimateBytesForDoubleArray(currentLength));
        }
    }

    static long estimateBytesForDoubleArray(long arraySize) {
        return RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + arraySize * (long) Double.BYTES;
    }

    @Override
    public void extraClose() {
        Releasables.closeExpectNoException(values);
    }
}
