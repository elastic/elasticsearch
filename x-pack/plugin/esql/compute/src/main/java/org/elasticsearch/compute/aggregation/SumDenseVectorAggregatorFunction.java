/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * Aggregator function for summing dense vectors (non-grouping).
 * Computes element-wise sum of all dense vectors.
 * All vectors must have the same dimensions.
 */
public class SumDenseVectorAggregatorFunction implements AggregatorFunction {

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("sum", ElementType.FLOAT, "dense_vector")
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final SumDenseVectorAggregatorState state;
    private final List<Integer> channels;
    private final DriverContext driverContext;
    private float[] buffer;

    public SumDenseVectorAggregatorFunction(List<Integer> channels, DriverContext driverContext) {
        this.state = new SumDenseVectorAggregatorState();
        this.channels = channels;
        this.driverContext = driverContext;
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        FloatBlock block = page.getBlock(channels.get(0));
        for (int i = 0; i < page.getPositionCount(); i++) {
            if (mask.getBoolean(i)) {
                addDenseVector(block, i);
            }
        }
    }

    @Override
    public void addIntermediateInput(Page page) {
        FloatBlock block = page.getBlock(channels.get(0));
        if (block.areAllValuesNull()) {
            return;
        }
        for (int i = 0; i < block.getPositionCount(); i++) {
            addDenseVector(block, i);
        }
    }

    private void addDenseVector(FloatBlock block, int i) {
        if (block.isNull(i) == false) {
            int valueCount = block.getValueCount(i);
            state.add(block, block.getFirstValueIndex(i), valueCount);
        }
    }

    @Override
    public int intermediateBlockCount() {
        return 1;
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        state.toIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        if (state.getSeen()) {
            float[] sum = state.getSum();
            if (sum != null) {
                try (FloatBlock.Builder builder = driverContext.blockFactory().newFloatBlockBuilder(1)) {
                    builder.beginPositionEntry();
                    for (float f : sum) {
                        builder.appendFloat(f);
                    }
                    builder.endPositionEntry();
                    blocks[offset] = builder.build();
                }
                return;
            }
        }
        blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }
}
