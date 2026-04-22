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
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

public class CountAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("count", ElementType.LONG),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final DriverContext driverContext;
    private final LongState state;
    private final List<ExpressionEvaluator> inputs;
    private final boolean countAll;

    CountAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
        this.driverContext = driverContext;
        this.inputs = inputs;
        this.state = new LongState(0);
        this.countAll = inputs.isEmpty();
        boolean success = false;
        try {
            driverContext.breaker().addEstimateBytesAndMaybeBreak(ExpressionEvaluator.totalRamBytesUsed(inputs), "ESQL");
            success = true;
        } finally {
            if (success == false) {
                this.state.close();
            }
        }
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (countAll) {
            // this will work also when the page has no blocks
            if (mask.isConstant() && mask.getBoolean(0)) {
                state.longValue(state.longValue() + page.getPositionCount());
                return;
            }
            int count = 0;
            for (int i = 0; i < mask.getPositionCount(); i++) {
                if (mask.getBoolean(i)) {
                    count++;
                }
            }
            state.longValue(state.longValue() + count);
            return;
        }
        try (Block block = inputs.getFirst().eval(page)) {
            if (block.areAllValuesNull()) {
                return;
            }
            if (mask.isConstant()) {
                if (mask.getBoolean(0) == false) {
                    return;
                }
                state.longValue(state.longValue() + getBlockTotalValueCount(block));
                return;
            }
            state.longValue(state.longValue() + countMasked(block, mask));
        }
    }

    /**
     * Returns the number of total values in a block
     * @param block block to count values for
     * @return number of total values present in the block
     */
    protected int getBlockTotalValueCount(Block block) {
        return block.getTotalValueCount();
    }

    private int countMasked(Block block, BooleanVector mask) {
        int count = 0;
        if (countAll) {
            for (int p = 0; p < block.getPositionCount(); p++) {
                if (mask.getBoolean(p)) {
                    count++;
                }
            }
            return count;
        }
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (mask.getBoolean(p)) {
                count += getBlockValueCountAtPosition(block, p);
            }
        }
        return count;
    }

    /**
     * Returns the number of values at a given position in a block
     * @param block block
     * @param position position to get the number of values
     * @return
     */
    protected int getBlockValueCountAtPosition(Block block, int position) {
        return block.getValueCount(position);
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert inputs.size() == intermediateBlockCount();
        try (LongBlock count = (LongBlock) inputs.getFirst().eval(page); BooleanBlock seen = (BooleanBlock) inputs.get(1).eval(page)) {
            if (count.areAllValuesNull()) {
                return;
            }
            assert count.getPositionCount() == 1;
            assert count.getPositionCount() == seen.getPositionCount();
            state.longValue(state.longValue() + count.getLong(0));
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        state.toIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = driverContext.blockFactory().newConstantLongBlockWith(state.longValue(), 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("inputs=").append(inputs);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(
            state,
            Releasables.wrap(inputs),
            () -> driverContext.breaker().addWithoutBreaking(-ExpressionEvaluator.totalRamBytesUsed(inputs))
        );
    }

    public static AggregatorFunctionSupplier supplier() {
        return new CountAggregatorFunctionSupplier();
    }

    protected static class CountAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            return CountAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return CountGroupingAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
            return new CountAggregatorFunction(driverContext, inputs);
        }

        @Override
        public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return new CountGroupingAggregatorFunction(channels, driverContext);
        }

        @Override
        public String describe() {
            return "count";
        }
    }
}
