/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreakingException;

import java.util.Arrays;

/**
 * Block build of PointBlocks.
 * This class is generated. Do not edit it.
 */
final class PointBlockBuilder extends AbstractBlockBuilder implements PointBlock.Builder {

    private double[] xValues, yValues;

    PointBlockBuilder(int estimatedSize, BlockFactory blockFactory) {
        super(blockFactory);
        int initialSize = Math.max(estimatedSize, 2);
        adjustBreaker(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + initialSize * elementSize());
        xValues = new double[initialSize];
        yValues = new double[initialSize];
    }

    @Override
    public PointBlockBuilder appendPoint(double x, double y) {
        ensureCapacity();
        xValues[valueCount] = x;
        yValues[valueCount] = y;
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
        return xValues.length;
    }

    @Override
    protected void growValuesArray(int newSize) {
        xValues = Arrays.copyOf(xValues, newSize);
        yValues = Arrays.copyOf(yValues, newSize);
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
                appendPoint(vector.getX(p), vector.getY(p));
            }
        } else {
            for (int p = 0; p < positionCount; p++) {
                int count = block.getValueCount(p);
                int i = block.getFirstValueIndex(p);
                for (int v = 0; v < count; v++, i++) {
                    appendPoint(block.getX(i), block.getY(i));
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
            for (int v = 0; v < count; v++, i++) {
                appendPoint(block.getX(i), block.getY(i));
            }
            if (count > 1) {
                endPositionEntry();
            }
        }
    }

    private void copyFromVector(PointVector vector, int beginInclusive, int endExclusive) {
        for (int p = beginInclusive; p < endExclusive; p++) {
            appendPoint(vector.getX(p), vector.getY(p));
        }
    }

    @Override
    public PointBlockBuilder mvOrdering(Block.MvOrdering mvOrdering) {
        this.mvOrdering = mvOrdering;
        return this;
    }

    @Override
    public PointBlock build() {
        try {
            finish();
            PointBlock theBlock;
            if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
                theBlock = blockFactory.newConstantPointBlockWith(xValues[0], yValues[0], 1, estimatedBytes);
            } else {
                if (valuesLength() - valueCount > 1024 || valueCount < (valuesLength() / 2)) {
                    growValuesArray(valueCount);
                }
                if (isDense() && singleValued()) {
                    theBlock = blockFactory.newPointArrayVector(xValues, yValues, positionCount, estimatedBytes).asBlock();
                } else {
                    theBlock = blockFactory.newPointArrayBlock(
                        xValues,
                        yValues,
                        positionCount,
                        firstValueIndexes,
                        nullsMask,
                        mvOrdering,
                        estimatedBytes
                    );
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
