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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Releasables;

import java.util.List;

public class PresentAggregatorFunction implements AggregatorFunction {
    public static AggregatorFunctionSupplier supplier() {
        return new AggregatorFunctionSupplier() {
            @Override
            public List<IntermediateStateDesc> nonGroupingIntermediateStateDesc() {
                return PresentAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public List<IntermediateStateDesc> groupingIntermediateStateDesc() {
                return PresentGroupingAggregatorFunction.intermediateStateDesc();
            }

            @Override
            public AggregatorFunction aggregator(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
                return new PresentAggregatorFunction(driverContext, inputs);
            }

            @Override
            public GroupingAggregatorFunction groupingAggregator(DriverContext driverContext, List<Integer> channels) {
                return new PresentGroupingAggregatorFunction(channels, driverContext);
            }

            @Override
            public String describe() {
                return "present";
            }
        };
    }

    private static final List<IntermediateStateDesc> INTERMEDIATE_STATE_DESC = List.of(
        new IntermediateStateDesc("present", ElementType.BOOLEAN)
    );

    public static List<IntermediateStateDesc> intermediateStateDesc() {
        return INTERMEDIATE_STATE_DESC;
    }

    private final DriverContext driverContext;
    private final List<ExpressionEvaluator> inputs;

    private boolean state;

    PresentAggregatorFunction(DriverContext driverContext, List<ExpressionEvaluator> inputs) {
        this.driverContext = driverContext;
        this.inputs = inputs;
        this.state = false;
        driverContext.breaker().addEstimateBytesAndMaybeBreak(ExpressionEvaluator.totalRamBytesUsed(inputs), "ESQL");
    }

    @Override
    public int intermediateBlockCount() {
        return intermediateStateDesc().size();
    }

    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        // is a previous page has non-null values we can return immediately
        if (state) return;
        if (mask.isConstant() && mask.getBoolean(0) == false) return;

        try (Block block = inputs.getFirst().eval(page)) {
            this.state = mask.isConstant() ? block.getTotalValueCount() > 0 : presentMasked(block, mask);
        }
    }

    private boolean presentMasked(Block block, BooleanVector mask) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            if (mask.getBoolean(p)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void addIntermediateInput(Page page) {
        assert inputs.size() == intermediateBlockCount();
        try (BooleanBlock present = (BooleanBlock) inputs.getFirst().eval(page)) {
            if (present.areAllValuesNull()) {
                return;
            }
            assert present.getPositionCount() == 1;
            if (present.getBoolean(0)) {
                this.state = true;
            }
        }
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        evaluateFinal(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        blocks[offset] = driverContext.blockFactory().newConstantBooleanBlockWith(state, 1);
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
            Releasables.wrap(inputs),
            () -> driverContext.breaker().addWithoutBreaking(-ExpressionEvaluator.totalRamBytesUsed(inputs))
        );
    }
}
