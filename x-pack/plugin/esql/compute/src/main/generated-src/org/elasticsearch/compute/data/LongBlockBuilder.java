/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

// begin generated imports
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of LongBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
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
     * <p>
     *     For single-position copies see {@link #copyFrom(LongBlock, int)}.
     * </p>
     */
    @Override
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
            copyFrom(block, p);
        }
    }

    private void copyFromVector(LongVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendLong(vector.getLong(p));
        }
    }

    /**
     * Copy the values in {@code block} at {@code position}. If this position
     * has a single value, this'll copy a single value. If this positions has
     * many values, it'll copy all of them. If this is {@code null}, then it'll
     * copy the {@code null}.
     * <p>
     *     Note that there isn't a version of this method on {@link Block.Builder} that takes
     *     {@link Block}. That'd be quite slow, running position by position. And it's important
     *     to know if you are copying {@link BytesRef}s so you can have the scratch.
     * </p>
     */
    @Override
    public LongBlockBuilder copyFrom(LongBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendLong(block.getLong(i++));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendLong(block.getLong(i++));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public LongBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private LongBlock buildBigArraysBlock() {
        final LongBlock theBlock;
        final LongArray array = blockFactory.bigArrays().newLongArray(valueCount, false);
        for (int i = 0; i < valueCount; i++) {
            array.set(i, values[i]);
        }
        if (isDense() && singleValued()) {
            theBlock = new LongBigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new LongBigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
    public LongBlock build() {
        try {
            finish();
            LongBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantLongBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newLongArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newLongArrayBlock(
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
