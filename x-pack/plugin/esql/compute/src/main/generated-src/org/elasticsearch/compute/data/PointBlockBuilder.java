/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.common.util.ObjectArray;

import java.util.Arrays;

/**
 * Block build of PointBlocks.
 * This class is generated. Do not edit it.
 */
final class PointBlockBuilder extends AbstractBlockBuilder implements PointBlock.Builder {

    private SpatialPoint[] values;

    PointBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        values = new SpatialPoint[initialSize];
    }

    @Override
    public PointBlockBuilder appendPoint(SpatialPoint value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
    }

    @Override
    protected int elementSize() {
        return 16;
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
    public PointBlockBuilder appendNull() {
        super.appendNull();
        return this;
    }

    @Override
    public PointBlockBuilder beginPositionEntry() {
        super.beginPositionEntry();
        return this;
    }

    @Override
    public PointBlockBuilder endPositionEntry() {
        super.endPositionEntry();
        return this;
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public PointBlockBuilder appendAllValuesToCurrentPosition(Block block) {
        if (block.areAllValuesNull()) {
            return appendNull();
        }
        return appendAllValuesToCurrentPosition((PointBlock) block);
    }

    /**
     * Appends the all values of the given block into a the current position
     * in this builder.
     */
    @Override
    public PointBlockBuilder appendAllValuesToCurrentPosition(PointBlock block) {
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
        final PointVector vector = block.asVector();
        if (vector != null) {
            for (int p = 0; p < positionCount; p++) {
                appendPoint(vector.getPoint(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++) {
                    appendPoint(block.getPoint(i++));
                }
            }
        }
        if (totalValueCount > 1) {
            endPositionEntry();
        }
        return this;
    }

    @Override
    public PointBlockBuilder copyFrom(Block block, int beginInclusive, int endExclusive) {
        if (block.areAllValuesNull()) {
            for (int p = beginInclusive; p < endExclusive; p++) {
                appendNull();
            }
            return this;
        }
        return copyFrom((PointBlock) block, beginInclusive, endExclusive);
    }

    /**
     * Copy the values in {@code block} from {@code beginInclusive} to
     * {@code endExclusive} into this builder.
     */
    public PointBlockBuilder copyFrom(PointBlock block, int beginInclusive, int endExclusive) {
        if (endExclusive > block.getPositionCount()) {
            throw new IllegalArgumentException("can't copy past the end [" + endExclusive + " > " + block.getPositionCount() + "]");
        }
        PointVector vector = block.asVector();
        if (vector != null) {
            copyFromVector(vector, beginInclusive, endExclusive);
        } else {
            copyFromBlock(block, beginInclusive, endExclusive);
        }
        return this;
    }

    private void copyFromBlock(PointBlock block, int beginInclusive, int endExclusive) {
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
                appendPoint(block.getPoint(i++));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(PointVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendPoint(vector.getPoint(p));
        }
    }

    @Override
    public PointBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    private PointBlock buildBigArraysBlock() {
        final PointBlock theBlock;
        final ObjectArray<SpatialPoint> array = blockFactory.bigArrays().newObjectArray(valueCount);
        for (int i = 0; i < valueCount; i++) {
            array.set(i, values[i]);
        }
        if (isDense() && singleValued()) {
            theBlock = new PointBigArrayVector(array, positionCount, blockFactory).asBlock();
        } else {
            theBlock = new PointBigArrayBlock(array, positionCount, firstValueIndexes, nullsMask, mvOrdering, blockFactory);
        }
        /*
        * Update the breaker with the actual bytes used.
        * We pass false below even though we've used the bytes. That's weird,
        * but if we break here we will throw away the used memory, letting
        * it be deallocated. The exception will bubble up and the builder will
        * still technically be open, meaning the calling code should close it
        * which will return all used memory to the breaker.
        */
        blockFactory.adjustBreaker(theBlock.ramBytesUsed() - estimatedBytes - array.ramBytesUsed(), false);
        return theBlock;
    }

    @Override
    public PointBlock build() {
        try {
            finish();
            PointBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantPointBlockWith(values[0], 1, estimatedBytes);
            } else {
                if (estimatedBytes > blockFactory.maxPrimitiveArrayBytes()) {
                    theBlock = buildBigArraysBlock();
                } else {
                    if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                        adjustBreaker(valueCount * elementSize());
                        values = Arrays.copyOf(values, valueCount);
                        adjustBreaker(-values.length * elementSize());
                    }
                    if (isDense() && singleValued()) {
                        theBlock = blockFactory.newPointArrayVector(values, positionCount, estimatedBytes).asBlock();
                    } else {
                        theBlock = blockFactory.newPointArrayBlock(
                            values,
                            positionCount,
                            firstValueIndexes,
                            nullsMask,
                            mvOrdering,
                            estimatedBytes
                        );
                    }
                }
            }
            built();
            return theBlock;
        } catch (CircuitBreakingException e) {
            close();
            throw e;
        }
    }
}
