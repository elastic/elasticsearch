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
import org.elasticsearch.common.util.BitArray;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of BooleanBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class BooleanBlockBuilder extends AbstractBlockBuilder implements BooleanBlock.Builder {

    private boolean[] values;

    BooleanBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new boolean[initialSize];
    }

    @Override
    public BooleanBlockBuilder appendBoolean(boolean value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return Byte.BYTES;
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
     * <p>
     *     For single-position copies see {@link #copyFrom(BooleanBlock, int)}.
     * </p>
     */
    @Override
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
            copyFrom(block, p);
        }
    }

    private void copyFromVector(BooleanVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendBoolean(vector.getBoolean(p));
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
    public BooleanBlockBuilder copyFrom(BooleanBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendBoolean(block.getBoolean(i++));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendBoolean(block.getBoolean(i++));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public BooleanBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private BooleanBlock buildBigArraysBlock() {
        final BooleanBlock theBlock;
        final BitArray array = new BitArray(valueCount, blockFactory.bigArrays());
        for (int i = 0; i < valueCount; i++) {
            if (values[i]) {
                array.set(i);
            }
        }
        if (isDense() && singleValued()) {
            theBlock = new BooleanBigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new BooleanBigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
    public BooleanBlock build() {
        try {
            finish();
            BooleanBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantBooleanBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newBooleanArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newBooleanArrayBlock(
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
