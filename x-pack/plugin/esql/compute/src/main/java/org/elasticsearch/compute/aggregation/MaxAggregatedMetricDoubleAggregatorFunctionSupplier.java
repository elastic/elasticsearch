/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

/**
 * {@link AggregatorFunctionSupplier} implementation for {@link MaxDoubleAggregator} with aggregated double metric field.
 */
public final class MaxAggregatedMetricDoubleAggregatorFunctionSupplier implements AggregatorFunctionSupplier {

    private static final int MAX_COMPOSITE_SUBBLOCK = 1;

    private final List<Integer> channels;

    public MaxAggregatedMetricDoubleAggregatorFunctionSupplier(List<Integer> channels) {
        this.channels = channels;
    }

    @Override
    public AggregatorFunction aggregator(DriverContext driverContext) {
        // Copied from MaxDoubleAggregatorFunction, see change comments for actual differences:
        final DoubleState state = new DoubleState(MaxDoubleAggregator.init());
        return new AggregatorFunction() {

            static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
                new IntermediateStateDesc("max", ElementType.DOUBLE),
                new IntermediateStateDesc("seen", ElementType.BOOLEAN)
            );

            public static List<IntermediateStateDesc> intermediateStateDesc() {
                return INTERMEDIATE_STATE_DESC;
            }

            @Override
            public int intermediateBlockCount() {
                return INTERMEDIATE_STATE_DESC.size();
            }

            @Override
            public void addRawInput(Page page, BooleanVector mask) {
                if (mask.allFalse()) {
                    // Entire page masked away
                    return;
                }
                if (mask.allTrue()) {
                    // No masking
                    // CHANGE:
                    CompositeBlock compositeBlock = page.getBlock(channels.get(0));
                    DoubleBlock block = compositeBlock.getBlock(MAX_COMPOSITE_SUBBLOCK);
                    // END CHANGE:
                    DoubleVector vector = block.asVector();
                    if (vector != null) {
                        addRawVector(vector);
                    } else {
                        addRawBlock(block);
                    }
                    return;
                }
                // Some positions masked away, others kept
                // CHANGE:
                CompositeBlock compositeBlock = page.getBlock(channels.get(0));
                DoubleBlock block = compositeBlock.getBlock(MAX_COMPOSITE_SUBBLOCK);
                // END CHANGE:
                DoubleVector vector = block.asVector();
                if (vector != null) {
                    addRawVector(vector, mask);
                } else {
                    addRawBlock(block, mask);
                }
            }

            private void addRawVector(DoubleVector vector) {
                state.seen(true);
                for (int i = 0; i < vector.getPositionCount(); i++) {
                    state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), vector.getDouble(i)));
                }
            }

            private void addRawVector(DoubleVector vector, BooleanVector mask) {
                state.seen(true);
                for (int i = 0; i < vector.getPositionCount(); i++) {
                    if (mask.getBoolean(i) == false) {
                        continue;
                    }
                    state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), vector.getDouble(i)));
                }
            }

            private void addRawBlock(DoubleBlock block) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    if (block.isNull(p)) {
                        continue;
                    }
                    state.seen(true);
                    int start = block.getFirstValueIndex(p);
                    int end = start + block.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), block.getDouble(i)));
                    }
                }
            }

            private void addRawBlock(DoubleBlock block, BooleanVector mask) {
                for (int p = 0; p < block.getPositionCount(); p++) {
                    if (mask.getBoolean(p) == false) {
                        continue;
                    }
                    if (block.isNull(p)) {
                        continue;
                    }
                    state.seen(true);
                    int start = block.getFirstValueIndex(p);
                    int end = start + block.getValueCount(p);
                    for (int i = start; i < end; i++) {
                        state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), block.getDouble(i)));
                    }
                }
            }

            @Override
            public void addIntermediateInput(Page page) {
                assert channels.size() == intermediateBlockCount();
                assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
                Block maxUncast = page.getBlock(channels.get(0));
                if (maxUncast.areAllValuesNull()) {
                    return;
                }
                DoubleVector max = ((DoubleBlock) maxUncast).asVector();
                assert max.getPositionCount() == 1;
                Block seenUncast = page.getBlock(channels.get(1));
                if (seenUncast.areAllValuesNull()) {
                    return;
                }
                BooleanVector seen = ((BooleanBlock) seenUncast).asVector();
                assert seen.getPositionCount() == 1;
                if (seen.getBoolean(0)) {
                    state.doubleValue(MaxDoubleAggregator.combine(state.doubleValue(), max.getDouble(0)));
                    state.seen(true);
                }
            }

            @Override
            public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
                state.toIntermediate(blocks, offset, driverContext);
            }

            @Override
            public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
                if (state.seen() == false) {
                    blocks[offset] = driverContext.blockFactory().newConstantNullBlock(1);
                    return;
                }
                blocks[offset] = driverContext.blockFactory().newConstantDoubleBlockWith(state.doubleValue(), 1);
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

        };
    }

    @Override
    public MaxDoubleGroupingAggregatorFunction groupingAggregator(DriverContext driverContext) {
        // TODO:
        throw new UnsupportedOperationException("grouping aggregator is not supported yet");
        // return MaxDoubleGroupingAggregatorFunction.create(channels, driverContext);
    }

    @Override
    public String describe() {
        return "max of aggregated doubles";
    }
}
