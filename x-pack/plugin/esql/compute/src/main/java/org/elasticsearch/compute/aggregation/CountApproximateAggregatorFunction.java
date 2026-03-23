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
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.DoubleVector;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class CountApproximateAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("count", ElementType.DOUBLE),
        new IntermediateStateDesc("seen", ElementType.BOOLEAN)
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final DoubleState state;
    private final List<Integer> channels;
    private final boolean countAll;

    public static CountApproximateAggregatorFunction create(List<Integer> inputChannels) {
        return new CountApproximateAggregatorFunction(inputChannels, new DoubleState(0));
    }

    protected CountApproximateAggregatorFunction(List<Integer> channels, DoubleState state) {
        this.channels = channels;
        this.state = state;
        // no channels specified means count-all/count(*)
        this.countAll = channels.isEmpty();
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    private int blockIndex() {
        // In case of countAll, block index is irrelevant.
        // Page.positionCount should be used instead,
        // because the page could have zero blocks
        // (drop all columns scenario)
        return countAll ? -1 : channels.get(0);
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (countAll) {
            // this will work also when the page has no blocks
            if (mask.isConstant() && mask.getBoolean(0)) {
                state.doubleValue(state.doubleValue() + page.getPositionCount());
            } else {
                int count = 0;
                for (int i = 0; i < mask.getPositionCount(); i++) {
                    if (mask.getBoolean(i)) {
                        count++;
                    }
                }
                state.doubleValue(state.doubleValue() + count);
            }
        } else {
            Block block = page.getBlock(blockIndex());
            DoubleState state = this.state;
            int count;
            if (mask.isConstant()) {
                if (mask.getBoolean(0) == false) {
                    return;
                }
                count = getBlockTotalValueCount(block);
            } else {
                count = countMasked(block, mask);
            }
            state.doubleValue(state.doubleValue() + count);
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
        assert channels.size() == intermediateBlockCount();
        var blockIndex = blockIndex();
        assert page.getBlockCount() >= blockIndex + intermediateStateDesc().size();
        Block uncastBlock = page.getBlock(channels.get(0));
        if (uncastBlock.areAllValuesNull()) {
            return;
        }
        DoubleVector count = page.<DoubleBlock>getBlock(channels.get(0)).asVector();
        BooleanVector seen = page.<BooleanBlock>getBlock(channels.get(1)).asVector();
        assert count.getPositionCount() == 1;
        assert count.getPositionCount() == seen.getPositionCount();
        state.doubleValue(state.doubleValue() + count.getDouble(0));
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        state.toIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = driverContext.blockFactory().newConstantDoubleBlockWith(state.doubleValue(), 1);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("channels=").append(channels);
        sb.append("]");
        return sb.toString();
    }

    @Override
    public void close() {
        state.close();
    }

    public static AggregatorFunctionSupplier supplier() {
        return new CountApproximateAggregatorFunctionSupplier();
    }

    protected static class CountApproximateAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            return CountApproximateAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return CountApproximateGroupingAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            return CountApproximateAggregatorFunction.create(channels);
        }

        @Override
        public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return CountApproximateGroupingAggregatorFunction.create(driverContext, channels);
        }

        @Override
        public String describe() {
            return "count";
        }
    }
}
