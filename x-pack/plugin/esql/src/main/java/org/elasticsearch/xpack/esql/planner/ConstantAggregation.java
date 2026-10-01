/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.compute.aggregation.AggregatorFunction;
import org.elasticsearch.compute.aggregation.AggregatorFunctionSupplier;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.evaluator.mapper.EvaluatorMapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Runs an aggregation's own compute aggregator at plan time over constant input, so the optimizer can fold aggregations over
 * constants without restating what each aggregation does with them.
 */
public final class ConstantAggregation {

    /**
     * What an aggregator produces for a repeated constant input row.
     *
     * @param empty        the result over zero rows
     * @param single       the result over one row
     * @param inputIgnored the row leaves the aggregator's state untouched, so any number of rows yields {@code empty}
     * @param idempotent   repeating the row leaves the aggregator's state untouched, so any number of rows {@code >= 1} yields
     *                     {@code single}
     */
    public record Probe(Object empty, Object single, boolean inputIgnored, boolean idempotent) {}

    private ConstantAggregation() {}

    /**
     * The result of {@code aggregation} over zero rows.
     */
    public static Object empty(ToAggregator aggregation, int inputCount, Source source, FoldContext foldContext) {
        DriverContext driverContext = EvaluatorMapper.foldDriverContext(source, foldContext);
        try (AggregatorFunction aggregator = aggregation.supplier().aggregator(driverContext, channels(inputCount))) {
            Object value = finalValue(aggregator, driverContext);
            driverContext.finish();
            return value;
        }
    }

    /**
     * Feeds one row made of {@code inputs} to {@code aggregation}, once, twice and merged with itself, and compares the
     * intermediate states. Returns {@code null} if the aggregator emitted a warning: at plan time the rows might never exist.
     */
    @Nullable
    public static Probe probe(ToAggregator aggregation, List<Object> inputs, Source source, FoldContext foldContext) {
        DriverContext driverContext = EvaluatorMapper.foldDriverContext(source, foldContext);
        AggregatorFunctionSupplier supplier = aggregation.supplier();
        List<Integer> inputChannels = channels(inputs.size());
        List<Block> toRelease = new ArrayList<>();
        try (
            AggregatorFunction none = supplier.aggregator(driverContext, inputChannels);
            AggregatorFunction one = fed(supplier.aggregator(driverContext, inputChannels), inputs, 1, driverContext);
            AggregatorFunction two = fed(supplier.aggregator(driverContext, inputChannels), inputs, 2, driverContext);
            AggregatorFunction merged = supplier.aggregator(driverContext, channels(none.intermediateBlockCount()));
            AggregatorFunction noneFinal = supplier.aggregator(driverContext, inputChannels);
            AggregatorFunction oneFinal = fed(supplier.aggregator(driverContext, inputChannels), inputs, 1, driverContext)
        ) {
            Block[] noneState = intermediate(none, driverContext, toRelease);
            Block[] oneState = intermediate(one, driverContext, toRelease);
            Block[] twoState = intermediate(two, driverContext, toRelease);
            merged.addIntermediateInput(new Page(1, oneState));
            merged.addIntermediateInput(new Page(1, oneState));
            Block[] mergedState = intermediate(merged, driverContext, toRelease);

            Object empty = finalValue(noneFinal, driverContext);
            Object single = finalValue(oneFinal, driverContext);
            driverContext.finish();
            if (driverContext.warnings().isEmpty() == false) {
                return null;
            }
            boolean inputIgnored = Arrays.equals(oneState, noneState);
            boolean idempotent = Arrays.equals(twoState, oneState) && Arrays.equals(mergedState, oneState);
            return new Probe(empty, single, inputIgnored, idempotent);
        } finally {
            Releasables.close(toRelease);
        }
    }

    private static AggregatorFunction fed(AggregatorFunction aggregator, List<Object> inputs, int rows, DriverContext driverContext) {
        Block[] blocks = new Block[inputs.size()];
        BooleanVector mask = null;
        boolean success = false;
        try {
            for (int i = 0; i < blocks.length; i++) {
                blocks[i] = BlockUtils.constantBlock(driverContext.blockFactory(), inputs.get(i), rows);
            }
            mask = driverContext.blockFactory().newConstantBooleanVector(true, rows);
            aggregator.addRawInput(new Page(rows, blocks), mask);
            success = true;
            return aggregator;
        } finally {
            Releasables.close(Releasables.wrap(blocks), mask);
            if (success == false) {
                aggregator.close();
            }
        }
    }

    private static Block[] intermediate(AggregatorFunction aggregator, DriverContext driverContext, List<Block> toRelease) {
        Block[] blocks = new Block[aggregator.intermediateBlockCount()];
        try {
            aggregator.evaluateIntermediate(blocks, 0, driverContext);
        } finally {
            toRelease.addAll(Arrays.asList(blocks));
        }
        return blocks;
    }

    private static Object finalValue(AggregatorFunction aggregator, DriverContext driverContext) {
        Block[] blocks = new Block[1];
        try {
            aggregator.evaluateFinal(blocks, 0, driverContext);
            return BlockUtils.toJavaObject(blocks[0], 0);
        } finally {
            Releasables.close(blocks);
        }
    }

    private static List<Integer> channels(int count) {
        return IntStream.range(0, count).boxed().toList();
    }
}
