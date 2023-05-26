/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Block build of DoubleBlocks.
 * This class is generated. Do not edit it.
 */
final class DoubleBlockBuilder extends AbstractBlockBuilder implements DoubleBlock.Builder {

    private double[] values;

    DoubleBlockBuilder(int estimatedSize) {
        values = new double[Math.max(estimatedSize, 2)];
    }

    @Override
    public DoubleBlockBuilder appendDouble(double value) {
        ensureCapacity();
        values[valueCount] = value;
        hasNonNullValue = true;
        valueCount++;
        updatePosition();
        return this;
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

    @Override
    public DoubleBlock build() {
        finish();
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
            return new ConstantDoubleVector(values[0], 1).asBlock();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            if (isDense() && singleValued()) {
                return new DoubleArrayVector(values, positionCount).asBlock();
            } else {
                return new DoubleArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering);
            }
        }
    }
}
