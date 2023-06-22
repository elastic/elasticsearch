/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import java.util.Arrays;

/**
 * Block build of BooleanBlocks.
 * This class is generated. Do not edit it.
 */
final class BooleanBlockBuilder extends AbstractBlockBuilder implements BooleanBlock.Builder {

    private boolean[] values;

    BooleanBlockBuilder(int estimatedSize) {
        values = new boolean[Math.max(estimatedSize, 2)];
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

    @Override
    public BooleanBlock build() {
        finish();
        if (hasNonNullValue && positionCount == 1 && valueCount == 1) {
            return new ConstantBooleanVector(values[0], 1).asBlock();
        } else {
            if (values.length - valueCount > 1024 || valueCount < (values.length / 2)) {
                values = Arrays.copyOf(values, valueCount);
            }
            if (isDense() && singleValued()) {
                return new BooleanArrayVector(values, positionCount).asBlock();
            } else {
                return new BooleanArrayBlock(values, positionCount, firstValueIndexes, nullsMask, mvOrdering);
            }
        }
    }
}
