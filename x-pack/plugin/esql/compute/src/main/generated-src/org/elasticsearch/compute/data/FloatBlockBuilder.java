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
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
// end generated imports

/**
 * Block build of FloatBlocks.
 * This class is generated. Edit {@code X-BlockBuilder.java.st} instead.
 */
final class FloatBlockBuilder extends AbstractBlockBuilder implements FloatBlock.Builder {

    private float[] values;

    FloatBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new float[initialSize];
    }

    @Override
    public FloatBlockBuilder appendFloat(float value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return Float.BYTES;
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
    public FloatBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public FloatBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public FloatBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    @Override
    public FloatBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((FloatBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     * <p>
     *     For single-position copies see {@link #copyFrom(FloatBlock, int)}.
     * </p>
     */
    @Override
    public FloatBlockBuilder copyFrom(FloatBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        FloatVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(FloatBlock block, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            copyFrom(block, p);
        }
    }

    private void copyFromVector(FloatVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendFloat(vector.getFloat(p));
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
    public FloatBlockBuilder copyFrom(FloatBlock block, int position) {
        if (block.isNull(position)) {
            appendNull();
            return this;
        }
        int count = block.getValueCount(position);
        int i = block.getFirstValueIndex(position);
        if (count == 1) {
            appendFloat(block.getFloat(i++));
            return this;
        }
        beginPositionEntry();
        for (int v = 0; v < count; v++) {
            appendFloat(block.getFloat(i++));
        }
        endPositionEntry();
        return this;
    }

    @Override
    public FloatBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private FloatBlock buildBigArraysBlock() {
        final FloatBlock theBlock;
        final FloatArray array = blockFactory.bigArrays().newFloatArray(valueCount, false);
        for (int i = 0; i < valueCount; i++) {
            array.set(i, values[i]);
        }
        if (isDense() && singleValued()) {
            theBlock = new FloatBigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new FloatBigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
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
    public FloatBlock build() {
        try {
            finish();
            FloatBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantFloatBlockWith(values[0], 1, estimatedBytes);
            } else if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                theBlock = buildBigArraysBlock();
            } else if (isDense() && singleValued()) {
                theBlock = blockFactory.newFloatArrayVector(values, positionCount, estimatedBytes).asBlock();
            } else {
                theBlock = blockFactory.newFloatArrayBlock(
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
