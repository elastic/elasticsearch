/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FloatArray;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

/**
 * Grouping state for summing dense vectors. Each group maintains its own float[] for the sum.
 * All vectors must have the same dimensions.
 */
final class SumDenseVectorGroupingState extends AbstractFallibleArrayState implements GroupingAggregatorState {

    private FloatArray sums;
    private int dimensions = -1;

    SumDenseVectorGroupingState(BigArrays bigArrays) {
        super(bigArrays);
    }

    /**
     * Add a vector from a FloatBlock to the sum for a specific group.
     * @param groupId the group ID
     * @param block the FloatBlock containing the vector
     * @param start the starting index in the block
     * @param dimensions the number of values (dimensions) in the vector
     */
    void add(int groupId, FloatBlock block, int start, int dimensions) {
        if (this.dimensions == -1) {
            this.sums = bigArrays.newFloatArray(dimensions);
            this.dimensions = dimensions;
        } else if (dimensions != this.dimensions) {
            throw new IllegalArgumentException(
                "Cannot sum dense vectors with different dimensions: expected [" + this.dimensions + "] but got [" + dimensions + "]"
            );
        }
        ensureCapacity(groupId);

        int groupSumStart = groupId * dimensions;
        for (int i = 0; i < dimensions; i++) {
            float result = block.getFloat(start + i) + sums.get(groupSumStart + i);
            if (Float.isFinite(result) == false) {
                throw new ArithmeticException("not a finite float number: " + result);
            }
            sums.set(groupSumStart + i, result);
        }
        trackGroupId(groupId);
    }

    private void ensureCapacity(int groupId) {
        assert dimensions != -1 : "Dimensions should be set before ensuring capacity";
        sums = bigArrays.grow(sums, (groupId + 1L) * dimensions);
    }

    @Override
    public void toIntermediate(Block[] blocks, int offset, IntVector selected, DriverContext driverContext) {
        blocks[offset] = toValuesBlock(selected, driverContext);
        try (var hasFailedBuilder = driverContext.blockFactory().newBooleanVectorFixedBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                hasFailedBuilder.appendBoolean(i, hasFailed(group));
            }
            blocks[offset + 1] = hasFailedBuilder.build().asBlock();
        }
    }

    Block toValuesBlock(IntVector selected, DriverContext driverContext) {
        if (sums == null) {
            return driverContext.blockFactory().newConstantNullBlock(selected.getPositionCount());
        }
        try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(selected.getPositionCount())) {
            for (int i = 0; i < selected.getPositionCount(); i++) {
                int group = selected.getInt(i);
                int groupIndex = group * dimensions;
                if (groupIndex < sums.size() && hasValue(group) && hasFailed(group) == false) {
                    builder.beginPositionEntry();
                    for (int j = 0; j < dimensions; j++) {
                        builder.appendFloat(sums.get(groupIndex + j));
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
