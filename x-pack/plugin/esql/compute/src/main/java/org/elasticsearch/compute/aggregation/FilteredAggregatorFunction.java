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
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.data.ToMask;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.core.Releasables;

/**
 * A {@link AggregatorFunction} that wraps another, filtering which positions
 * are supplied to the aggregator.
 * <p>
 *     This works by running the filter and providing its results in the {@code mask}
 *     parameter for {@link #addRawInput}.
 * </p>
 */
record FilteredAggregatorFunction(AggregatorFunction next, EvalOperator.ExpressionEvaluator filter) implements AggregatorFunction {
    @Override
    public void addRawInput(Page page, BooleanVector mask) {
        if (mask.isConstant() == false || mask.getBoolean(0) == false) {
            throw new UnsupportedOperationException("can't filter twice");
        }
        try (BooleanBlock filterResult = ((BooleanBlock) filter.eval(page)); ToMask m = filterResult.toMask()) {
            // TODO warn on mv fields
            next.addRawInput(page, m.mask());
        }
    }

    @Override
    public void addIntermediateInput(Page page) {
        next.addIntermediateInput(page);
    }

    @Override
    public void evaluateIntermediate(Block[] blocks, int offset, DriverContext driverContext) {
        next.evaluateIntermediate(blocks, offset, driverContext);
    }

    @Override
    public void evaluateFinal(Block[] blocks, int offset, DriverContext driverContext) {
        next.evaluateFinal(blocks, offset, driverContext);
    }

    @Override
    public int intermediateBlockCount() {
        return next.intermediateBlockCount();
    }

    @Override
    public void close() {
        Releasables.close(next, filter);
    }
}
