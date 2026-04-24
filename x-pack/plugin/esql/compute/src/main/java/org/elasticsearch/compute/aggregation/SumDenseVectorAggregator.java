/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.ann.Aggregator;
import org.elasticsearch.compute.ann.GroupingAggregator;
import org.elasticsearch.compute.ann.IntermediateState;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.operator.DriverContext;

/**
 * Aggregator for summing dense vectors element-wise.
 * All vectors must have the same dimensions.
 *
 * <p>This uses the block-only aggregation path where the combine method receives
 * the entire FloatBlock and a position, rather than individual float values.
 */
@Aggregator(
    value = { @IntermediateState(name = "sum", type = "FLOAT_BLOCK"), @IntermediateState(name = "failed", type = "BOOLEAN") },
    warnExceptions = ArithmeticException.class
)
@GroupingAggregator
class SumDenseVectorAggregator {

    public static String describe() {
        return "sum of dense_vectors";
    }

    // ========== Non-Grouping Methods ==========

    public static SingleState initSingle() {
        return new SingleState();
    }

    /**
     * Combine a dense vector into the current sum.
     * The FloatBlock contains the vector at position p as multi-valued floats.
     * This signature matches the block-only path: combine(state, position, block)
     */
    public static void combine(SingleState current, @Position int p, FloatBlock vector) {
        if (vector.isNull(p)) {
            return;
        }
        int valueCount = vector.getValueCount(p);
        int start = vector.getFirstValueIndex(p);
        current.add(vector, start, valueCount);
    }

    /**
     * Combine intermediate state (a FloatBlock with sum vector) into current state.
     */
    public static void combineIntermediate(SingleState state, FloatBlock sum, boolean failed) {
        if (failed) {
            state.failed(failed);
        } else if (sum.areAllValuesNull() == false && sum.isNull(0) == false) {
            int valueCount = sum.getValueCount(0);
            int start = sum.getFirstValueIndex(0);
            state.add(sum, start, valueCount);
        }
    }

    public static Block evaluateFinal(SingleState state, DriverContext driverContext) {
        return state.toBlock(driverContext);
    }

    // ========== Grouping Methods ==========

    public static SumDenseVectorGroupingState initGrouping(BigArrays bigArrays) {
        return new SumDenseVectorGroupingState(bigArrays);
    }

    /**
     * Combine a dense vector into the state for a specific group.
     * This signature matches the block-only grouping path: combine(state, groupId, position, block)
     */
    public static void combine(SumDenseVectorGroupingState current, int groupId, @Position int p, FloatBlock vector) {
        if (vector.isNull(p)) {
            return;
        }
        int valueCount = vector.getValueCount(p);
        int start = vector.getFirstValueIndex(p);
        current.add(groupId, vector, start, valueCount);
    }

    /**
     * Combine intermediate state for a specific group.
     */
    public static void combineIntermediate(SumDenseVectorGroupingState current, int groupId, FloatBlock sum, boolean failed, int position) {
        if (failed) {
            current.setFailed(position);
        } else if (sum.isNull(position) == false) {
            int valueCount = sum.getValueCount(position);
            int start = sum.getFirstValueIndex(position);
            current.add(groupId, sum, start, valueCount);
        }
    }

    public static Block evaluateFinal(SumDenseVectorGroupingState state, IntVector selected, GroupingAggregatorEvaluationContext ctx) {
        return state.toValuesBlock(selected, ctx.driverContext());
    }

    // ========== State Classes ==========

    /**
     * State for non-grouping aggregation.
     */
    static class SingleState implements AggregatorState {
        private float[] sum;
        private boolean seen = false;
        private boolean failed = false;
        private int dimensions = -1;

        void add(FloatBlock block, int start, int valueCount) {
            if (failed) {
                return;
            }
            if (sum == null) {
                dimensions = valueCount;
                sum = new float[dimensions];
            }
            if (valueCount != dimensions) {
                throw new IllegalArgumentException(
                    "Cannot sum dense vectors with different dimensions: expected [" + dimensions + "] but got [" + valueCount + "]"
                );
            }
            for (int i = 0; i < dimensions; i++) {
                sum[i] += block.getFloat(start + i);
                if (Float.isFinite(sum[i]) == false) {
                    throw new ArithmeticException("not a finite float number: " + sum[i]);
                }
            }
            seen = true;
        }

        Block toBlock(DriverContext driverContext) {
            if (seen == false || failed || sum == null) {
                return driverContext.blockFactory().newConstantNullBlock(1);
            }
            for (float f : sum) {
                if (Float.isFinite(f) == false) {
                    return driverContext.blockFactory().newConstantNullBlock(1);
                }
            }
            try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(sum.length)) {
                builder.beginPositionEntry();
                for (float f : sum) {
                    builder.appendFloat(f);
                }
                builder.endPositionEntry();
                return builder.build();
            }
        }

        public void failed(boolean failed) {
            this.failed = failed;
        }

        @Override
        public void toIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
            blocks[offset] = toBlock(driverContext);
            blocks[offset + 1] = driverContext.blockFactory().newConstantBooleanBlockWith(failed, 1);
        }

        @Override
        public void close() {}
    }
}
