/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.CompositeBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.LongVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;

import java.util.List;

public class SparklineAggregatorFunction implements AggregatorFunction {
    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("aggregator_intermediate_states", ElementType.COMPOSITE)
    );

    private final DriverContext driverContext;

    private final SparklineAggregator.SingleState state;

    private final List<Integer> channels;

    public SparklineAggregatorFunction(DriverContext driverContext, List<Integer> channels, SparklineAggregator.SingleState state) {
        this.driverContext = driverContext;
        this.channels = channels;
        this.state = state;
    }

    public static SparklineAggregatorFunction create(
        DriverContext driverContext,
        List<Integer> channels,
        Rounding.Prepared dateBucketRounding,
        long minDate,
        long maxDate,
        AggregatorFunctionSupplier supplier
    ) {
        return new SparklineAggregatorFunction(
            driverContext,
            channels,
            SparklineAggregator.initSingle(driverContext.bigArrays(), dateBucketRounding, minDate, maxDate, supplier)
        );
    }

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
        } else if (mask.allTrue()) {
            addRawInputNotMasked(page);
        } else {
            addRawInputMasked(page, mask);
        }
    }

    private void addRawInputMasked(Page page, BooleanVector mask) {
        LongBlock trendValueBlock = page.getBlock(channels.get(0));
        LongBlock dateValueBlock = channels.size() == 2 ? page.getBlock(channels.get(1)) : page.getBlock(channels.get(0));
        // TODO: Is there a better way to handle COUNT(*) than just passing the date value to both trend and date?
        LongVector trendValueVector = trendValueBlock.asVector();
        if (trendValueVector == null) {
            addRawBlock(trendValueBlock, dateValueBlock, mask);
            return;
        }
        LongVector dateValueVector = dateValueBlock.asVector();
        if (dateValueVector == null) {
            addRawBlock(trendValueBlock, dateValueBlock, mask);
            return;
        }
        addRawVector(trendValueVector, dateValueVector, mask);
    }

    private void addRawInputNotMasked(Page page) {
        LongBlock trendValueBlock = page.getBlock(channels.get(0));
        LongBlock dateValueBlock = channels.size() == 2 ? page.getBlock(channels.get(1)) : page.getBlock(channels.get(0));
        LongVector trendValueVector = trendValueBlock.asVector();
        if (trendValueVector == null) {
            addRawBlock(trendValueBlock, dateValueBlock);
            return;
        }
        LongVector dateValueVector = dateValueBlock.asVector();
        if (dateValueVector == null) {
            addRawBlock(trendValueBlock, dateValueBlock);
            return;
        }
        addRawVector(trendValueVector, dateValueVector);
    }

    private void addRawVector(LongVector trendValueVector, LongVector dateValueVector) {
        for (int valuesPosition = 0; valuesPosition < trendValueVector.getPositionCount(); valuesPosition++) {
            long trendValueValue = trendValueVector.getLong(valuesPosition);
            long dateValueValue = dateValueVector.getLong(valuesPosition);
            SparklineAggregator.combine(driverContext, state, trendValueValue, dateValueValue);
        }
    }

    private void addRawVector(LongVector trendValueVector, LongVector dateValueVector, BooleanVector mask) {
        for (int valuesPosition = 0; valuesPosition < trendValueVector.getPositionCount(); valuesPosition++) {
            if (mask.getBoolean(valuesPosition) == false) {
                continue;
            }
            long trendValueValue = trendValueVector.getLong(valuesPosition);
            long dateValueValue = dateValueVector.getLong(valuesPosition);
            SparklineAggregator.combine(driverContext, state, trendValueValue, dateValueValue);
        }
    }

    private void addRawBlock(LongBlock trendValueBlock, LongBlock dateValueBlock) {
        for (int p = 0; p < trendValueBlock.getPositionCount(); p++) {
            int trendValueValueCount = trendValueBlock.getValueCount(p);
            if (trendValueValueCount == 0) {
                continue;
            }
            int dateValueValueCount = dateValueBlock.getValueCount(p);
            if (dateValueValueCount == 0) {
                continue;
            }
            int trendValueStart = trendValueBlock.getFirstValueIndex(p);
            int trendValueEnd = trendValueStart + trendValueValueCount;
            for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
                long trendValueValue = trendValueBlock.getLong(trendValueOffset);
                int dateValueStart = dateValueBlock.getFirstValueIndex(p);
                int dateValueEnd = dateValueStart + dateValueValueCount;
                for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
                    long dateValueValue = dateValueBlock.getLong(dateValueOffset);
                    SparklineAggregator.combine(driverContext, state, trendValueValue, dateValueValue);
                }
            }
        }
    }

    private void addRawBlock(LongBlock trendValueBlock, LongBlock dateValueBlock, BooleanVector mask) {
        for (int p = 0; p < trendValueBlock.getPositionCount(); p++) {
            if (mask.getBoolean(p) == false) {
                continue;
            }
            int trendValueValueCount = trendValueBlock.getValueCount(p);
            if (trendValueValueCount == 0) {
                continue;
            }
            int dateValueValueCount = dateValueBlock.getValueCount(p);
            if (dateValueValueCount == 0) {
                continue;
            }
            int trendValueStart = trendValueBlock.getFirstValueIndex(p);
            int trendValueEnd = trendValueStart + trendValueValueCount;
            for (int trendValueOffset = trendValueStart; trendValueOffset < trendValueEnd; trendValueOffset++) {
                long trendValueValue = trendValueBlock.getLong(trendValueOffset);
                int dateValueStart = dateValueBlock.getFirstValueIndex(p);
                int dateValueEnd = dateValueStart + dateValueValueCount;
                for (int dateValueOffset = dateValueStart; dateValueOffset < dateValueEnd; dateValueOffset++) {
                    long dateValueValue = dateValueBlock.getLong(dateValueOffset);
                    SparklineAggregator.combine(driverContext, state, trendValueValue, dateValueValue);
                }
            }
        }
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert channels.size() == intermediateBlockCount();
        assert page.getBlockCount() >= channels.get(0) + intermediateStateDesc().size();
        Block aggregatorIntermediateStatesUncast = page.getBlock(channels.get(0));
        if (aggregatorIntermediateStatesUncast.areAllValuesNull()) {
            return;
        }
        CompositeBlock compositeBlock = (CompositeBlock) aggregatorIntermediateStatesUncast;
        SparklineAggregator.combineIntermediate(driverContext, state, compositeBlock);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        state.toIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = SparklineAggregator.evaluateFinal(state, driverContext);
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

    public static class SparklineAggregatorFunctionSupplier implements AggregatorFunctionSupplier {
        private final Rounding.Prepared dateBucketRounding;

        private final long minDate;

        private final long maxDate;

        private final AggregatorFunctionSupplier supplier;

        public SparklineAggregatorFunctionSupplier(
            Rounding.Prepared dateBucketRounding,
            long minDate,
            long maxDate,
            AggregatorFunctionSupplier supplier
        ) {
            this.dateBucketRounding = dateBucketRounding;
            this.minDate = minDate;
            this.maxDate = maxDate;
            this.supplier = supplier;
        }

        @Override
        public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
            return SparklineAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
            return SparklineGroupingAggregatorFunction.intermediateStateDesc();
        }

        @Override
        public AggregatorFunction aggregator(DriverContext driverContext, List<Integer> channels) {
            return SparklineAggregatorFunction.create(driverContext, channels, dateBucketRounding, minDate, maxDate, supplier);
        }

        @Override
        public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
            return SparklineGroupingAggregatorFunction.create(channels, driverContext, dateBucketRounding, minDate, maxDate, supplier);
        }

        @Override
        public String describe() {
            return "sparkline";
        }
    }
}
