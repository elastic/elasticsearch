/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.aggregation.Aggregator;
import org.elasticsearch.compute.aggregation.Aggregator.Factory;
import org.elasticsearch.compute.aggregation.AggregatorMode;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/**
 * Blocking aggregation operator.  An aggregation operator aggregates its input with one or more
 * aggregator functions, e.g. avg, max, etc, and outputs a Page containing the results of those
 * aggregations.
 *
 * The operator is blocking in the sense that it only produces output once all possible input has
 * been added, that is, when the {@link #finish} method has been called.
 */
public class AggregationOperator implements Operator {

    private boolean finished;
    private Page output;
    private final List<Aggregator> aggregators;
    private final DriverContext driverContext;

    public record AggregationOperatorFactory(List<Factory> aggregators, AggregatorMode mode) implements OperatorFactory {

        @Override
        public Operator get(DriverContext driverContext) {
            return new AggregationOperator(aggregators.stream().map(x -> x.apply(driverContext)).toList(), driverContext);
        }

        @Override
        public String toString() {
            return describe();
        }

        @Override
        public String describe() {
            return "AggregationOperator[mode = "
                + mode
                + ", aggs = "
                + aggregators.stream().map(Factory::describe).collect(joining(", "))
                + "]";
        }
    }

    public AggregationOperator(List<Aggregator> aggregators, DriverContext driverContext) {
        Objects.requireNonNull(aggregators);
        checkNonEmpty(aggregators);
        this.aggregators = aggregators;
        this.driverContext = driverContext;
    }

    @Override
    public boolean needsInput() {
        return finished == false;
    }

    @Override
    public void addInput(Page page) {
        checkState(needsInput(), "Operator is already finishing");
        requireNonNull(page, "page is null");
        try {
            for (Aggregator aggregator : aggregators) {
                aggregator.processPage(page);
            }
        } finally {
            page.releaseBlocks();
        }
    }

    @Override
    public Page getOutput() {
        Page p = output;
        this.output = null;
        return p;
    }

    @Override
    public void finish() {
        if (finished) {
            return;
        }
        finished = true;
        Block[] blocks = null;
        boolean success = false;
        try {
            int[] aggBlockCounts = aggregators.stream().mapToInt(Aggregator::evaluateBlockCount).toArray();
            // TODO: look into allocating the blocks lazily
            blocks = new Block[Arrays.stream(aggBlockCounts).sum()];
            int offset = 0;
            for (int i = 0; i < aggregators.size(); i++) {
                var aggregator = aggregators.get(i);
                aggregator.evaluate(blocks, offset, driverContext);
                offset += aggBlockCounts[i];
            }
            output = new Page(blocks);
            success = true;
        } finally {
            if (success == false && blocks != null) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    @Override
    public boolean isFinished() {
        return finished && output == null;
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(() -> {
            if (output != null) {
                Releasables.closeExpectNoException(() -> output.releaseBlocks());
            }
        }, Releasables.wrap(aggregators));
    }

    private static void checkState(boolean condition, String msg) {
        if (condition == false) {
            throw new IllegalArgumentException(msg);
        }
    }

    private static void checkNonEmpty(List<?> list) {
        if (list.size() < 1) {
            throw new IllegalArgumentException("empty list");
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(this.getClass().getSimpleName()).append("[");
        sb.append("aggregators=").append(aggregators).append("]");
        return sb.toString();
    }
}
