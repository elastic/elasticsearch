/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.IntArray;

import java.util.Arrays;

/**
 * Block build of IntBlocks.
 * This class is generated. Do not edit it.
 */
final class IntBlockBuilder extends AbstractBlockBuilder implements IntBlock.Builder {

    private int[] values;

    IntBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new int[initialSize];
    }

    @Override
    public IntBlockBuilder appendInt(int value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return Integer.BYTES;
    }

    @Override
    protected int valuesLength() {
        return values.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        values = Arrays.copyOf(values, newSize);
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

    private IntBlock buildBigArraysBlock() {
        final IntBlock theBlock;
        final IntArray array = blockFactory.bigArrays().newIntArray(valueCount, false);
        for (int i = 0; i < valueCount; i++) {
            array.set(i, values[i]);
        }
        if (isDense() && singleValued()) {
            theBlock = new IntBigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new IntBigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - array.ramBytesUsed());
        return theBlock;
    }

    @Override
    public IntBlock build() {
        try {
            finish();
            IntBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantIntBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newIntArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newIntArrayBlock(
                    values, // stylecheck
                    positionCount,
                    firstValueIndexes,
                    nullsMask,
                    mvOrdering,
                    estimatedBytes
                );
            }
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }
}
