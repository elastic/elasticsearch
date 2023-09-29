/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

/**
 * Block build of LongBlocks.
 * This class is generated. Do not edit it.
 */
final class LongBlockBuilder extends AbstractBlockBuilder implements LongBlock.Builder {

    private long[] values;

    LongBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new long[initialSize];
    }

    @Override
    public LongBlockBuilder appendLong(long value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return Long.BYTES;
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
    public LongBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public LongBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public LongBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public LongBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((LongBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public LongBlockBuilder appendAllValuesToCurrentPosition(LongBlock block) {
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
        final LongVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendLong(vector.getLong(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendLong(block.getLong(i++));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public LongBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((LongBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public LongBlockBuilder copyFrom(LongBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        LongVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(LongBlock block, int beginInclusive, int endExclusive) {
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
                appendLong(block.getLong(i++));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(LongVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendLong(vector.getLong(p));
        }
    }

    @Override
    public LongBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    @Override
    public LongBlock build() {
        finish();
        LongBlock block;
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
            block = new ConstantLongVector(values[0], 1, blockFactory).asBlock();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            if (isDense() && singleValued()) {
                block = new LongArrayVector(values, positionCount, blockFactory).asBlock();
            } else {
                block = new LongArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
            }
        }
        /*
         * Update the breaker with the actual bytes used.
         * We pass false below even though we've used the bytes. That's weird,
         * but if we break here we will throw away the used memory, letting
         * it be deallocated. The exception will bubble up and the builder will
         * still technically be open, meaning the calling code should close it
         * which will return all used memory to the breaker.
         */
        blockFactory.adjustBreaker(block.ramBytesUsed() - estimatedBytes, false);
        built();
        return block;
    }
}
