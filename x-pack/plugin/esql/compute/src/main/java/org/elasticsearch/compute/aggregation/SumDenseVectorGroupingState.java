/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Grouping state for summing dense vectors. Each group maintains its own float[] for the sum.
 * All vectors must have the same dimensions.
 */
final class SumDenseVectorGroupingState extends AbstractArrayState implements GroupingAggregatorState {

    private ObjectArray<float[]> sums;
    private int expectedDimensions = -1;

    SumDenseVectorGroupingState(BigArrays bigArrays) {
        super(bigArrays);
        this.sums = bigArrays.newObjectArray(1);
    }

    void add(int groupId, float[] vector) {
        if (vector == null) {
            return;
        }
        ensureCapacity(groupId);

        if (expectedDimensions == -1) {
            expectedDimensions = vector.length;
        } else if (vector.length != expectedDimensions) {
            throw new IllegalArgumentException(
                "Cannot sum dense vectors with different dimensions: expected [" + expectedDimensions + "] but got [" + vector.length + "]"
            );
        }

        float[] currentSum = sums.get(groupId);
        if (currentSum == null) {
            currentSum = new float[vector.length];
            sums.set(groupId, currentSum);
        }

        for (int i = 0; i < vector.length; i++) {
            currentSum[i] += vector[i];
        }
        trackGroupId(groupId);
    }

    float[] get(int groupId) {
        if (groupId >= sums.size()) {
            return null;
        }
        return sums.get(groupId);
    }

    private void ensureCapacity(int groupId) {
        sums = bigArrays.grow(sums, groupId + 1);
    }

    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                float[] sum = group < sums.size() ? sums.get(group) : null;
                if (sum != null && hasValue(group)) {
                    builder.beginPositionEntry();
                    for (float f : sum) {
                        builder.appendFloat(f);
                    }
                    builder.endPositionEntry();
                } else {
                    builder.appendNull();
                }
            }
            blocks[offset] = builder.build();
        }
    }

    Block toValuesBlock(IntVector selected, DriverContext driverContext) {
        try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                float[] sum = group < sums.size() ? sums.get(group) : null;
                if (sum != null && hasValue(group)) {
                    builder.beginPositionEntry();
                    for (float f : sum) {
                        builder.appendFloat(f);
                    }
                    builder.endPositionEntry();
                } else {
                    builder.appendNull();
                }
            }
            return builder.build();
        }
    }

    @Override
    public void close() {
        Releasables.close(sums, super::close);
    }
}
